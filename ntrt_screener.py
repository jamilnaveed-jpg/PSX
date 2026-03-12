#!/usr/bin/env python3
"""
NTRT/MTRT Daily Screener — US Markets (Nasdaq + NYSE)
======================================================
Runs Mon–Fri to find stocks reporting earnings that match the
NTRT/MAGNA53 framework from the _AI_promptUse_.pdf playbook.

Data sources (all free, no API key required):
  - Earnings calendar : earningswhispers.com scrape OR financialmodelingprep.com
  - Fundamental data  : financialmodelingprep.com (free tier — 250 req/day)
  - Price / gap data  : Yahoo Finance JSON API (no key)

Usage:
  python ntrt_screener.py [--fmp-key YOUR_KEY] [--date 2026-03-12]

Output:
  ntrt_data.json  — consumed by ntrt.html

Setup (GitHub Actions):
  - Schedule: '30 16 * * 1-5'  (4:30 PM ET — after US close, Mon–Fri)
  - Also run: '30 8 * * 1-5'   (8:30 AM ET — catches pre-market movers)
  - Env var: FMP_API_KEY (optional — improves data quality)

Without FMP key: uses Yahoo Finance only (still functional).
With FMP key   : adds EPS surprise, analyst upgrades, institutional data.
"""

import os
import sys
import json
import time
import argparse
import datetime
import requests
from typing import Optional

# ── Config ────────────────────────────────────────────────────────────────────
DATA_FILE   = "ntrt_data.json"
MAX_HISTORY = 60           # keep last 60 scan days

# NTRT / MAGNA53 thresholds (from playbook)
SETUP_A_MIN_PRICE_GAIN   = 4.0     # % gap up
SETUP_A_MIN_VOLUME       = 100_000
SETUP_A_MIN_REV_GROWTH   = 29.0    # % YoY
SETUP_A_MIN_ANNUAL_SALES = 25_000_000

SETUP_B_MIN_EPS_GROWTH   = 100.0   # % YoY
SETUP_B_MIN_SALES_GROWTH = 100.0   # % YoY
SETUP_B_MIN_EPS_SURPRISE = 100.0   # %
SETUP_B_PREF_REV_GROWTH  = 25.0    # preferred
SETUP_B_MIN_REV_GROWTH   = 10.0    # minimum acceptable

SETUP_C_MIN_EPS_SURPRISE = 100.0
SETUP_C_MIN_REV_GROWTH   = 10.0
SETUP_C_MIN_ANNUAL_SALES = 25_000_000
SETUP_C_MIN_UPGRADES     = 3

MAGNA_M_EPS_GROWTH       = 100.0
MAGNA_M_SALES_GROWTH     = 100.0
MAGNA_M_EPS_SURPRISE     = 100.0
MAGNA_G_GAP              = 4.0
MAGNA_G_VOLUME           = 100_000
MAGNA_A_REV_GROWTH       = 25.0

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/html, */*",
}

# ── Argument parsing ──────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="NTRT/MTRT Daily Screener")
    p.add_argument("--fmp-key", default=os.environ.get("FMP_API_KEY", ""), help="FinancialModelingPrep API key")
    p.add_argument("--date",    default="",   help="Override date YYYY-MM-DD (default: today)")
    p.add_argument("--demo",    action="store_true", help="Insert demo data for UI testing")
    return p.parse_args()

# ── Date helpers ──────────────────────────────────────────────────────────────
def scan_date(override="") -> str:
    if override:
        return override
    return datetime.date.today().isoformat()

def is_weekday(d: str) -> bool:
    dt = datetime.date.fromisoformat(d)
    return dt.weekday() < 5

# ── Yahoo Finance helpers ─────────────────────────────────────────────────────
def yf_quote(ticker: str) -> dict:
    """Fetch live quote from Yahoo Finance (no key needed)."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=5d"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code != 200:
            return {}
        data = r.json()
        meta = data.get("chart", {}).get("result", [{}])[0].get("meta", {})
        return {
            "price":          meta.get("regularMarketPrice"),
            "prev_close":     meta.get("chartPreviousClose") or meta.get("previousClose"),
            "volume":         meta.get("regularMarketVolume"),
            "market_cap":     meta.get("marketCap"),
            "currency":       meta.get("currency", "USD"),
        }
    except Exception as e:
        print(f"  [yf_quote] {ticker}: {e}")
        return {}

def yf_earnings_history(ticker: str) -> dict:
    """Fetch earnings history from Yahoo Finance."""
    url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=earnings,financialData,defaultKeyStatistics,earningsTrend"
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        if r.status_code != 200:
            return {}
        result = r.json().get("quoteSummary", {}).get("result", [{}])
        if not result:
            return {}
        return result[0]
    except Exception as e:
        print(f"  [yf_earnings] {ticker}: {e}")
        return {}

def yf_income(ticker: str) -> dict:
    """Fetch income statement from Yahoo Finance."""
    url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=incomeStatementHistory,incomeStatementHistoryQuarterly,earningsHistory"
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        if r.status_code != 200:
            return {}
        result = r.json().get("quoteSummary", {}).get("result", [{}])
        if not result:
            return {}
        return result[0]
    except Exception as e:
        print(f"  [yf_income] {ticker}: {e}")
        return {}

# ── FMP helpers ───────────────────────────────────────────────────────────────
BASE_FMP = "https://financialmodelingprep.com/api"

def fmp_get(path: str, key: str, params: dict = None) -> list | dict:
    if not key:
        return []
    try:
        p = params or {}
        p["apikey"] = key
        r = requests.get(f"{BASE_FMP}{path}", params=p, headers=HEADERS, timeout=12)
        if r.status_code != 200:
            return []
        return r.json()
    except Exception as e:
        print(f"  [fmp] {path}: {e}")
        return []

def fmp_earnings_calendar(date: str, key: str) -> list:
    """Get earnings calendar for a specific date."""
    data = fmp_get(f"/v3/earning_calendar", key, {"from": date, "to": date})
    if isinstance(data, list):
        return data
    return []

def fmp_income_statement(ticker: str, key: str) -> list:
    data = fmp_get(f"/v3/income-statement/{ticker}", key, {"limit": 8, "period": "quarter"})
    return data if isinstance(data, list) else []

def fmp_analyst_estimates(ticker: str, key: str) -> list:
    data = fmp_get(f"/v3/analyst-estimates/{ticker}", key, {"limit": 4, "period": "quarter"})
    return data if isinstance(data, list) else []

def fmp_rating(ticker: str, key: str) -> dict:
    data = fmp_get(f"/v3/rating/{ticker}", key)
    if isinstance(data, list) and data:
        return data[0]
    return {}

# ── Earnings calendar (multi-source) ─────────────────────────────────────────
def get_earnings_calendar_yahoo(date: str) -> list:
    """
    Yahoo Finance earnings calendar — free, no key.
    Returns list of tickers with timing info.
    """
    url = f"https://query2.finance.yahoo.com/v1/finance/trending/US"
    # Better: use the earnings calendar endpoint
    url = "https://query2.finance.yahoo.com/v1/finance/screener/predefined/saved"
    # Use the earnings calendar scraper
    cal_url = f"https://query1.finance.yahoo.com/v1/finance/screener?crumb=&formatted=true&lang=en-US&region=US&scrIds=bf16ce0f-4bd8-4fb8-8bb9-d13f7ecf31b1"

    # Most reliable free source — Yahoo Finance earnings calendar page JSON
    try:
        url = "https://query2.finance.yahoo.com/v2/finance/calendar/earnings"
        params = {"date": date, "size": 200, "offset": 0}
        r = requests.get(url, params=params, headers={
            **HEADERS,
            "Referer": "https://finance.yahoo.com/calendar/earnings",
        }, timeout=15)
        if r.status_code == 200:
            data = r.json()
            rows = data.get("earnings", {}).get("rows", [])
            tickers = []
            for row in rows:
                sym = row.get("ticker", "")
                if sym and row.get("startdatetimetype"):
                    timing = row["startdatetimetype"]  # "AMC" or "BMO" or "TNS"
                    tickers.append({
                        "ticker":   sym,
                        "timing":   timing,  # BMO=before open, AMC=after close
                        "company":  row.get("companyshortname", ""),
                        "eps_est":  row.get("epsestimate"),
                        "eps_act":  row.get("epsactual"),
                        "rev_est":  row.get("revenueestimate"),
                        "rev_act":  row.get("revenueactual"),
                    })
            print(f"  [yahoo_cal] {len(tickers)} earnings for {date}")
            return tickers
    except Exception as e:
        print(f"  [yahoo_cal] error: {e}")

    return []

def get_earnings_tickers(date: str, fmp_key: str) -> list:
    """
    Try FMP first (better data), fall back to Yahoo Finance calendar.
    Returns unified list of dicts.
    """
    tickers = []

    # Try FMP calendar
    if fmp_key:
        fmp_cal = fmp_earnings_calendar(date, fmp_key)
        for item in fmp_cal:
            sym = item.get("symbol", "")
            if not sym:
                continue
            timing_raw = item.get("time", "")
            if "amc" in timing_raw.lower() or "after" in timing_raw.lower():
                timing = "AMC"
            elif "bmo" in timing_raw.lower() or "before" in timing_raw.lower():
                timing = "BMO"
            else:
                timing = "TNS"
            tickers.append({
                "ticker":   sym,
                "timing":   timing,
                "company":  item.get("name", ""),
                "eps_est":  item.get("epsEstimated"),
                "eps_act":  item.get("eps"),
                "rev_est":  item.get("revenueEstimated"),
                "rev_act":  item.get("revenue"),
            })
        print(f"  [fmp_cal] {len(tickers)} earnings for {date}")

    # Fall back / supplement with Yahoo
    if not tickers:
        tickers = get_earnings_calendar_yahoo(date)

    # Filter: only large US exchanges (heuristic — skip OTC/ETFs)
    filtered = []
    for t in tickers:
        sym = t["ticker"]
        # Skip obvious non-stocks
        if len(sym) > 5 or "." in sym or "-" in sym:
            continue
        filtered.append(t)

    return filtered

# ── Fundamental analysis ──────────────────────────────────────────────────────
def compute_eps_surprise(eps_actual, eps_estimate) -> Optional[float]:
    """EPS surprise %: (actual - estimate) / |estimate| * 100"""
    if eps_actual is None or eps_estimate is None:
        return None
    if eps_estimate == 0:
        return None
    return round((eps_actual - eps_estimate) / abs(eps_estimate) * 100, 1)

def compute_growth(current, previous) -> Optional[float]:
    """YoY growth %"""
    if current is None or previous is None or previous == 0:
        return None
    return round((current - previous) / abs(previous) * 100, 1)

def analyse_ticker(info: dict, fmp_key: str) -> dict:
    """
    Full NTRT/MAGNA53 analysis for one ticker.
    Returns enriched dict with all criteria scored.
    """
    ticker  = info["ticker"]
    company = info.get("company", ticker)
    timing  = info.get("timing", "TNS")

    print(f"  [analyse] {ticker} ({timing}) ...")

    result = {
        "ticker":      ticker,
        "company":     company,
        "timing":      timing,
        "date":        info.get("date", ""),
        # prices
        "price":       None,
        "prev_close":  None,
        "gap_pct":     None,
        "volume":      None,
        "market_cap":  None,
        # earnings
        "eps_actual":  info.get("eps_act"),
        "eps_estimate":info.get("eps_est"),
        "eps_surprise":None,
        "eps_growth":  None,
        # revenue
        "rev_actual":  info.get("rev_act"),
        "rev_estimate":info.get("rev_est"),
        "rev_growth":  None,
        "rev_growth_q2":None,  # 2nd consecutive quarter
        "annual_revenue":None,
        # MA criteria
        "magna_m": False, "magna_m_detail": "",
        "magna_g": False, "magna_g_detail": "",
        "magna_n": False, "magna_n_detail": "",
        "magna_a": False, "magna_a_detail": "",
        "magna_3": False, "magna_3_detail": "",
        "magna_score": 0,
        # setup classification
        "setup_a": False,
        "setup_b": False,
        "setup_c": False,
        "setup_types": [],
        # extras
        "analyst_upgrades": 0,
        "short_interest":   None,
        "inst_holders":     None,
        "story":            "",
        "verdict":          "MONITOR",
        "neglect_strength": "none",  # none / partial / strong
    }

    # ── Price & gap data ─────────────────────────────────────────────────────
    time.sleep(0.3)   # be polite to Yahoo
    quote = yf_quote(ticker)
    if quote:
        result["price"]      = quote.get("price")
        result["prev_close"] = quote.get("prev_close")
        result["volume"]     = quote.get("volume")
        result["market_cap"] = quote.get("market_cap")
        if result["price"] and result["prev_close"] and result["prev_close"] > 0:
            result["gap_pct"] = round(
                (result["price"] - result["prev_close"]) / result["prev_close"] * 100, 2
            )

    # ── EPS surprise ─────────────────────────────────────────────────────────
    result["eps_surprise"] = compute_eps_surprise(
        result["eps_actual"], result["eps_estimate"]
    )

    # ── Revenue data from Yahoo ───────────────────────────────────────────────
    time.sleep(0.3)
    income = yf_income(ticker)
    quarterly = income.get("incomeStatementHistoryQuarterly", {}).get("incomeStatementHistory", [])
    annual    = income.get("incomeStatementHistory", {}).get("incomeStatementHistory", [])

    rev_q = []
    for q in quarterly[:4]:
        v = q.get("totalRevenue", {}).get("raw")
        if v:
            rev_q.append(v)

    if len(rev_q) >= 2:
        # Most recent vs same quarter last year (index 0 vs index 4 if available)
        # With 4 quarters: index 0 = latest, index 3 = ~1yr ago
        if len(rev_q) >= 4:
            result["rev_growth"] = compute_growth(rev_q[0], rev_q[3])
        elif len(rev_q) >= 2:
            result["rev_growth"] = compute_growth(rev_q[0], rev_q[1])

        if len(rev_q) >= 5:
            result["rev_growth_q2"] = compute_growth(rev_q[1], rev_q[4])

    # Try calendar revenue if Yahoo misses
    if result["rev_actual"] and result["rev_growth"] is None:
        pass  # Keep None — better than wrong calc

    # EPS growth YoY
    earn_hist = income.get("earningsHistory", {}).get("history", [])
    if len(earn_hist) >= 4:
        eps_latest = earn_hist[0].get("epsActual", {}).get("raw")
        eps_yago   = earn_hist[3].get("epsActual", {}).get("raw")
        if eps_latest is not None and eps_yago is not None and eps_yago != 0:
            result["eps_growth"] = round((eps_latest - eps_yago) / abs(eps_yago) * 100, 1)

    # Annual revenue
    if annual:
        ar = annual[0].get("totalRevenue", {}).get("raw")
        result["annual_revenue"] = ar

    # ── FMP enrichment ────────────────────────────────────────────────────────
    if fmp_key:
        time.sleep(0.2)
        inc = fmp_income_statement(ticker, fmp_key)
        if len(inc) >= 5:
            r0 = inc[0].get("revenue", 0)
            r4 = inc[4].get("revenue", 0)
            if r0 and r4:
                result["rev_growth"] = round((r0 - r4) / abs(r4) * 100, 1)
            if len(inc) >= 9:
                r1 = inc[1].get("revenue", 0)
                r5 = inc[5].get("revenue", 0)
                if r1 and r5:
                    result["rev_growth_q2"] = round((r1 - r5) / abs(r5) * 100, 1)
            if inc[0].get("revenue"):
                result["annual_revenue"] = sum(
                    q.get("revenue", 0) for q in inc[:4]
                )

        # Analyst estimates for EPS surprise
        ests = fmp_analyst_estimates(ticker, fmp_key)
        if ests and result["eps_surprise"] is None:
            est = ests[0]
            ea = result.get("eps_actual") or est.get("epsActual")
            ee = est.get("epsEstimated")
            result["eps_surprise"] = compute_eps_surprise(ea, ee)

    # ── Score MAGNA53 criteria ────────────────────────────────────────────────
    # M — Massive
    m_triggers = []
    if result["eps_growth"] and abs(result["eps_growth"]) >= MAGNA_M_EPS_GROWTH:
        m_triggers.append(f"EPS growth {result['eps_growth']:+.0f}% YoY")
    if result["rev_growth"] and result["rev_growth"] >= MAGNA_M_SALES_GROWTH:
        m_triggers.append(f"Rev growth {result['rev_growth']:+.0f}% YoY")
    if result["eps_surprise"] and abs(result["eps_surprise"]) >= MAGNA_M_EPS_SURPRISE:
        m_triggers.append(f"EPS surprise {result['eps_surprise']:+.0f}%")
    # 2 quarters ≥29%
    if (result["rev_growth"] and result["rev_growth"] >= 29
            and result["rev_growth_q2"] and result["rev_growth_q2"] >= 29):
        m_triggers.append(f"2Q rev ≥29% ({result['rev_growth']:.0f}% & {result['rev_growth_q2']:.0f}%)")
    result["magna_m"]        = bool(m_triggers)
    result["magna_m_detail"] = " · ".join(m_triggers) if m_triggers else "No massive metric"

    # G — Gap
    g = result["gap_pct"]
    vol = result["volume"]
    g_ok = g is not None and abs(g) >= MAGNA_G_GAP
    v_ok = vol is not None and vol >= MAGNA_G_VOLUME
    result["magna_g"]        = g_ok and v_ok
    result["magna_g_detail"] = (
        f"Gap {g:+.1f}%, Vol {_fmt_vol(vol)}" if g is not None else "Gap data pending"
    )

    # N — Neglect (heuristic — use market cap & gap size as proxy)
    neglect_signals = []
    mc = result.get("market_cap")
    if mc:
        if mc < 500_000_000:
            neglect_signals.append("Small-cap (<$500M)")
        elif mc < 2_000_000_000:
            neglect_signals.append("Mid-cap (<$2B)")
    if result["gap_pct"] and abs(result["gap_pct"]) >= 15:
        neglect_signals.append("Large gap suggests prior neglect")
    if result["inst_holders"] is not None and result["inst_holders"] < 30:
        neglect_signals.append(f"Low institutional holders ({result['inst_holders']})")

    if len(neglect_signals) >= 2:
        result["neglect_strength"] = "strong"
        result["magna_n"] = True
    elif neglect_signals:
        result["neglect_strength"] = "partial"
        result["magna_n"] = True
    else:
        result["neglect_strength"] = "none"
        result["magna_n"] = False
    result["magna_n_detail"] = " · ".join(neglect_signals) if neglect_signals else "Large-cap — neglect unlikely"

    # A — Acceleration
    a_ok = (result["rev_growth"] is not None and result["rev_growth"] >= MAGNA_A_REV_GROWTH)
    if not a_ok:
        a_ok = (result["rev_growth"] and result["rev_growth"] >= 29
                and result["rev_growth_q2"] and result["rev_growth_q2"] >= 29)
    result["magna_a"]        = a_ok
    rg = result["rev_growth"]
    rg2 = result["rev_growth_q2"]
    rg_s  = f"{rg:+.1f}%" if rg is not None else "N/A"
    rg2_s = f"{rg2:+.1f}%" if rg2 is not None else "—"
    result["magna_a_detail"] = f"Rev growth: {rg_s} (prev Q: {rg2_s})"

    # 3 — Analyst upgrades (best effort — we use a heuristic value if no key)
    # FMP rating gives a signal
    if fmp_key:
        rating = fmp_rating(ticker, fmp_key)
        score = rating.get("ratingScore", 0)
        # Analyst upgrade approximation
        result["magna_3"] = score >= 4
        result["magna_3_detail"] = f"FMP rating score: {score}/5"
    else:
        result["magna_3"]        = False
        result["magna_3_detail"] = "Analyst data requires FMP key"

    # Score
    criteria = ["magna_m", "magna_g", "magna_n", "magna_a", "magna_3"]
    result["magna_score"] = sum(1 for c in criteria if result[c])

    # ── Setup type classification ─────────────────────────────────────────────
    setups = []

    # Setup A: Growth Ignition
    if (result["gap_pct"] and result["gap_pct"] >= SETUP_A_MIN_PRICE_GAIN
            and result["volume"] and result["volume"] >= SETUP_A_MIN_VOLUME
            and result["rev_growth"] and result["rev_growth"] >= SETUP_A_MIN_REV_GROWTH
            and result["magna_n"]):
        setups.append("A")
        result["setup_a"] = True

    # Setup B: Massive Earnings Shock
    b_massive = (
        (result["eps_growth"] and abs(result["eps_growth"]) >= SETUP_B_MIN_EPS_GROWTH)
        or (result["rev_growth"] and result["rev_growth"] >= SETUP_B_MIN_SALES_GROWTH)
        or (result["eps_surprise"] and abs(result["eps_surprise"]) >= SETUP_B_MIN_EPS_SURPRISE)
    )
    b_rev_ok = (result["rev_growth"] is not None and result["rev_growth"] >= SETUP_B_MIN_REV_GROWTH)
    if b_massive and b_rev_ok and result["magna_n"]:
        setups.append("B")
        result["setup_b"] = True

    # Setup C: Analyst Driven
    if (result["eps_surprise"] and result["eps_surprise"] >= SETUP_C_MIN_EPS_SURPRISE
            and result["rev_growth"] and result["rev_growth"] >= SETUP_C_MIN_REV_GROWTH
            and result["annual_revenue"] and result["annual_revenue"] >= SETUP_C_MIN_ANNUAL_SALES
            and result["magna_n"]):
        setups.append("C")
        result["setup_c"] = True

    result["setup_types"] = setups

    # ── Verdict ───────────────────────────────────────────────────────────────
    score = result["magna_score"]
    if score >= 4 and setups:
        result["verdict"] = "STRONG"
    elif score >= 3 and setups:
        result["verdict"] = "WATCH"
    elif score >= 2:
        result["verdict"] = "MONITOR"
    else:
        result["verdict"] = "SKIP"

    # ── Auto story ────────────────────────────────────────────────────────────
    story_parts = []
    if result["eps_surprise"]:
        story_parts.append(f"EPS surprise: {result['eps_surprise']:+.0f}%")
    if result["rev_growth"]:
        story_parts.append(f"Revenue growth: {result['rev_growth']:+.1f}% YoY")
    if result["gap_pct"]:
        story_parts.append(f"Gap: {result['gap_pct']:+.1f}%")
    if result["neglect_strength"] in ("partial", "strong"):
        story_parts.append(f"Neglect: {result['neglect_strength']}")
    result["story"] = " · ".join(story_parts) if story_parts else "Insufficient data"

    return result

# ── Formatting ────────────────────────────────────────────────────────────────
def _fmt_vol(v) -> str:
    if v is None:
        return "—"
    if v >= 1_000_000:
        return f"{v/1_000_000:.1f}M"
    if v >= 1_000:
        return f"{v/1_000:.0f}K"
    return str(v)

# ── Demo data ─────────────────────────────────────────────────────────────────
def make_demo_entry(date: str) -> dict:
    """Generate realistic demo data for UI testing when market is closed."""
    return {
        "date":   date,
        "scanned_at": datetime.datetime.utcnow().isoformat() + "Z",
        "market": "DEMO",
        "total_earnings": 12,
        "total_screened":  5,
        "candidates": [
            {
                "ticker": "FSLY", "company": "Fastly Inc.", "timing": "AMC",
                "date": date, "price": 12.45, "prev_close": 8.90, "gap_pct": 39.9,
                "volume": 42_000_000, "market_cap": 1_650_000_000,
                "eps_actual": 0.12, "eps_estimate": 0.06, "eps_surprise": 500.0,
                "eps_growth": None, "rev_actual": 172_600_000, "rev_estimate": 160_000_000,
                "rev_growth": 23.0, "rev_growth_q2": 18.5, "annual_revenue": 680_000_000,
                "magna_m": True,  "magna_m_detail": "EPS surprise +500% · 2Q rev ≥29%",
                "magna_g": True,  "magna_g_detail": "Gap +39.9%, Vol 42.0M",
                "magna_n": True,  "magna_n_detail": "Mid-cap (<$2B) · Years of losses then first profitable FY",
                "magna_a": True,  "magna_a_detail": "Rev growth: +23.0% (prev Q: +18.5%)",
                "magna_3": True,  "magna_3_detail": "William Blair upgraded to Buy",
                "magna_score": 5,
                "setup_a": True, "setup_b": True, "setup_c": True,
                "setup_types": ["B", "C"],
                "analyst_upgrades": 2, "short_interest": None, "inst_holders": None,
                "story": "First profitable fiscal year. Security revenue +32% YoY. Net retention 110%. Full-year guidance $700-720M. Agentic AI traffic narrative.",
                "verdict": "STRONG", "neglect_strength": "strong",
            },
            {
                "ticker": "VIAV", "company": "Viavi Solutions Inc.", "timing": "BMO",
                "date": date, "price": 11.20, "prev_close": 9.03, "gap_pct": 24.0,
                "volume": 18_000_000, "market_cap": 2_100_000_000,
                "eps_actual": 0.23, "eps_estimate": 0.20, "eps_surprise": 17.0,
                "eps_growth": 81.0, "rev_actual": 310_000_000, "rev_estimate": 295_000_000,
                "rev_growth": 36.0, "rev_growth_q2": 28.0, "annual_revenue": 1_100_000_000,
                "magna_m": True,  "magna_m_detail": "EPS growth +81% YoY · Rev growth +36% YoY",
                "magna_g": True,  "magna_g_detail": "Gap +24.0%, Vol 18.0M",
                "magna_n": True,  "magna_n_detail": "Mid-cap (<$2B) · Long sideways base",
                "magna_a": True,  "magna_a_detail": "Rev growth: +36.0% (prev Q: +28.0%)",
                "magna_3": True,  "magna_3_detail": "Multiple analyst upgrades",
                "magna_score": 5,
                "setup_a": True, "setup_b": False, "setup_c": False,
                "setup_types": ["A"],
                "analyst_upgrades": 3, "short_interest": None, "inst_holders": None,
                "story": "Fiber & optical networking revival. Network & service enablement +36% YoY. Multiple analyst upgrades. Accumulation pattern forming.",
                "verdict": "STRONG", "neglect_strength": "strong",
            },
            {
                "ticker": "KEYS", "company": "Keysight Technologies", "timing": "BMO",
                "date": date, "price": 148.50, "prev_close": 131.90, "gap_pct": 12.6,
                "volume": 5_200_000, "market_cap": 24_500_000_000,
                "eps_actual": 2.12, "eps_estimate": 2.02, "eps_surprise": 5.0,
                "eps_growth": 45.0, "rev_actual": 1_350_000_000, "rev_estimate": 1_320_000_000,
                "rev_growth": 10.0, "rev_growth_q2": 8.5, "annual_revenue": 5_200_000_000,
                "magna_m": False, "magna_m_detail": "EPS surprise +5% — below 100% threshold",
                "magna_g": True,  "magna_g_detail": "Gap +12.6%, Vol 5.2M",
                "magna_n": False, "magna_n_detail": "Large-cap ($24B) — neglect unlikely",
                "magna_a": False, "magna_a_detail": "Rev growth: +10.0% (prev Q: +8.5%) — below 25% threshold",
                "magna_3": True,  "magna_3_detail": "Multiple analyst upgrades",
                "magna_score": 2,
                "setup_a": False, "setup_b": False, "setup_c": False,
                "setup_types": ["B/C"],
                "analyst_upgrades": 2, "short_interest": None, "inst_holders": None,
                "story": "Orders accelerating. Semi test recovery + aerospace/defense strength. Large-cap limits neglect play — more MTRT than NTRT.",
                "verdict": "MONITOR", "neglect_strength": "none",
            },
            {
                "ticker": "ASTH", "company": "Asthma Holdings Inc.", "timing": "BMO",
                "date": date, "price": 6.40, "prev_close": 4.80, "gap_pct": 33.3,
                "volume": 8_500_000, "market_cap": 1_100_000_000,
                "eps_actual": 0.22, "eps_estimate": 0.11, "eps_surprise": 108.0,
                "eps_growth": 220.0, "rev_actual": None, "rev_estimate": None,
                "rev_growth": 43.0, "rev_growth_q2": 38.0, "annual_revenue": 180_000_000,
                "magna_m": True,  "magna_m_detail": "EPS surprise +108% · EPS growth +220% YoY",
                "magna_g": True,  "magna_g_detail": "Gap +33.3%, Vol 8.5M",
                "magna_n": True,  "magna_n_detail": "Small-cap (<$500M market cap) · Low institutional coverage",
                "magna_a": True,  "magna_a_detail": "Rev growth: +43.0% (prev Q: +38.0%)",
                "magna_3": False, "magna_3_detail": "Some analyst coverage but <3 upgrades",
                "magna_score": 4,
                "setup_a": True, "setup_b": True, "setup_c": False,
                "setup_types": ["A", "B"],
                "analyst_upgrades": 1, "short_interest": 6.2, "inst_holders": 18,
                "story": "Best fit this week — clears all 3 layers: earnings explosion, 25%+ rev confirmation, neglect. Note material weakness in 10-K — assess before entry.",
                "verdict": "STRONG", "neglect_strength": "strong",
            },
            {
                "ticker": "AVGO", "company": "Broadcom Inc.", "timing": "AMC",
                "date": date, "price": 214.50, "prev_close": 204.90, "gap_pct": 4.7,
                "volume": 22_000_000, "market_cap": 1_000_000_000_000,
                "eps_actual": 1.60, "eps_estimate": 1.48, "eps_surprise": 8.0,
                "eps_growth": 29.0, "rev_actual": 19_300_000_000, "rev_estimate": 18_900_000_000,
                "rev_growth": 29.0, "rev_growth_q2": 25.0, "annual_revenue": 72_000_000_000,
                "magna_m": True,  "magna_m_detail": "AI revenue +106% YoY to $8.4B · Total rev +29%",
                "magna_g": True,  "magna_g_detail": "Gap +4.7%, Vol 22.0M",
                "magna_n": False, "magna_n_detail": "$1.5T market cap — institutional neglect very limited",
                "magna_a": True,  "magna_a_detail": "Rev growth: +29.0% (prev Q: +25.0%)",
                "magna_3": True,  "magna_3_detail": "Bernstein PT $525 from $475 — multiple others expected",
                "magna_score": 4,
                "setup_a": False, "setup_b": False, "setup_c": False,
                "setup_types": ["B"],
                "analyst_upgrades": 4, "short_interest": None, "inst_holders": None,
                "story": "AI revenue +106% YoY to $8.4B. Q2 guidance crushed $22B vs $20.4B consensus. Custom silicon narrative validated. Multi-day MTRT hold candidate, not classic NTRT.",
                "verdict": "WATCH", "neglect_strength": "none",
            },
        ]
    }

# ── Main runner ───────────────────────────────────────────────────────────────
def load_history() -> list:
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE) as f:
            return json.load(f)
    return []

def save_history(history: list):
    history = history[-MAX_HISTORY:]
    with open(DATA_FILE, "w") as f:
        json.dump(history, f, indent=2)
    print(f"  [save] {DATA_FILE} — {len(history)} scan days")

def run_daily_scan(date: str, fmp_key: str) -> dict:
    print(f"\n=== NTRT/MTRT Scan: {date} ===")

    if not is_weekday(date):
        print("  Weekend — skipping.")
        return None

    # Get earnings calendar
    tickers = get_earnings_tickers(date, fmp_key)
    if not tickers:
        print("  No earnings found for this date.")
        return {
            "date": date,
            "scanned_at": datetime.datetime.utcnow().isoformat() + "Z",
            "market": "US",
            "total_earnings": 0,
            "total_screened": 0,
            "candidates": [],
        }

    print(f"  Found {len(tickers)} earnings reports to analyse")

    # Limit to first 100 to stay within rate limits
    tickers = tickers[:100]

    candidates = []
    for info in tickers:
        info["date"] = date
        try:
            r = analyse_ticker(info, fmp_key)
            # Only include if has any meaningful data
            if r["gap_pct"] is not None or r["rev_growth"] is not None or r["eps_surprise"] is not None:
                candidates.append(r)
        except Exception as e:
            print(f"  [error] {info['ticker']}: {e}")
        time.sleep(0.5)

    # Sort: STRONG first, then WATCH, then by magna_score desc
    order = {"STRONG": 0, "WATCH": 1, "MONITOR": 2, "SKIP": 3}
    candidates.sort(key=lambda x: (order.get(x["verdict"], 9), -x.get("magna_score", 0)))

    print(f"  Candidates with data: {len(candidates)}")
    strong = [c for c in candidates if c["verdict"] == "STRONG"]
    print(f"  STRONG setups: {len(strong)}")

    return {
        "date":           date,
        "scanned_at":     datetime.datetime.utcnow().isoformat() + "Z",
        "market":         "US",
        "total_earnings": len(tickers),
        "total_screened": len(candidates),
        "candidates":     candidates,
    }

def main():
    args = parse_args()
    date = scan_date(args.date)

    if args.demo:
        print(f"[demo] Generating demo data for {date}")
        history = load_history()
        # Remove existing entry for same date
        history = [h for h in history if h.get("date") != date]
        history.append(make_demo_entry(date))
        save_history(history)
        print("[demo] Done.")
        return

    result = run_daily_scan(date, args.fmp_key)
    if result is None:
        return

    history = load_history()
    history = [h for h in history if h.get("date") != date]
    history.append(result)
    save_history(history)
    print("\n✓ Scan complete.")

if __name__ == "__main__":
    main()
