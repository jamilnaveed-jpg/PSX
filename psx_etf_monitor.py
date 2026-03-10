"""
PSX ETF Monitor — Scraper v2  (Holdings fix)
=====================================================================
Source: dps.psx.com.pk  (official PSX data portal)

FIX: Holdings are scraped from the "Underlying Basket Per 10,000 ETF Units"
section which contains stock tickers (ATRL, KEL, OGDC, etc.) — NOT the
"Key People" table which has chairman/CEO names.

The basket table always has <a href="/company/XXXX"> links for each ticker.
Strategy: locate "Underlying Basket" h2, skip any h3 sub-headings like
"Cash Component", grab the first table that contains /company/ links.

Holdings are enriched with live price/change_pct from psxterminal.

Saves to:
  etf_data/YYYY-MM-DD.json   — one file per trading day
  etf_data/index.json        — sorted list of available dates

Schedule: 5:30 PM PKT = 12:30 UTC, Mon-Fri
=====================================================================
"""

import requests, json, re, time, os
from datetime import datetime
from bs4 import BeautifulSoup

PSX_BASE  = "https://dps.psx.com.pk"
TERM_BASE = "https://psxterminal.com"
OUT_DIR   = "etf_data"

SCRAPE_HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "Chrome/122.0.0.0 Safari/537.36",
    "Accept":     "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":    PSX_BASE + "/",
}
API_HDR = {
    "User-Agent": "Mozilla/5.0",
    "Accept":     "application/json",
    "Origin":     TERM_BASE,
    "Referer":    TERM_BASE + "/",
}

ETF_SYMBOLS = [
    "JSMFETF", "MZNPETF", "MIIETF",  "ACIETF",
    "NBPGETF", "NITGETF", "JSGBETF", "UBLPETF",
]

ETF_FULL_NAMES = {
    "JSMFETF": "JS Momentum Factor ETF",
    "MZNPETF": "Meezan Pakistan ETF",
    "MIIETF":  "Mahaana Islamic Index ETF",
    "ACIETF":  "Alfalah Consumer Index ETF",
    "NBPGETF": "NBP Pakistan Growth ETF",
    "NITGETF": "NIT Pakistan Gateway ETF",
    "JSGBETF": "JS Global Banking Sector ETF",
    "UBLPETF": "UBL Pakistan Enterprise ETF",
}

ETF_INDEX_MAP = {
    "JSMFETF": "JSMFI",
    "MZNPETF": "MZNPI",
    "MIIETF":  "MII30",
    "ACIETF":  "ACI",
    "NBPGETF": "NBPPGI",
    "NITGETF": "NITPGI",
    "JSGBETF": "JSGBKTI",
    "UBLPETF": "UPP9",
}

INDEX_SYMBOLS = ["KSE100", "KMI30", "KSE30", "MII30"]

IMPORTANT_KW = [
    "dividend", "interim dividend", "rebalancing", "book closure",
    "financial result", "annual result", "bonus", "right shares",
    "fund manager report", "credit of", "completion of rebalancing",
]


def tx(el):
    return " ".join(el.get_text().split()) if el else ""

def num(s):
    if not s: return None
    c = re.sub(r"[Rs.,\s%]", "", str(s))
    try:    return float(c)
    except: return None

def is_important(title):
    tl = title.lower()
    return any(k in tl for k in IMPORTANT_KW)


def scrape_etf(symbol):
    url = f"{PSX_BASE}/etf/{symbol}"
    print(f"  [{symbol}] GET {url}")
    try:
        r = requests.get(url, headers=SCRAPE_HDR, timeout=25)
        r.raise_for_status()
    except Exception as e:
        print(f"  [{symbol}] fetch failed: {e}")
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    page = r.text
    full = soup.get_text(" ")
    res  = {"symbol": symbol, "name": ETF_FULL_NAMES.get(symbol, symbol)}

    # ── QUOTE ─────────────────────────────────────────────────────────────────
    try:
        q = {}
        m = re.search(r'Rs\.\s*([\d,]+\.?\d*)', page)
        if m: q["price"] = num(m.group(1))
        chg = re.search(r'([-+]?\d+\.\d+)\s*\n\s*\(([-+]?\d+\.\d+)%\)', page)
        if chg:
            q["change"]     = num(chg.group(1))
            q["change_pct"] = num(chg.group(2))
        for label, key in [("Open","open"),("High","high"),("Low","low"),
                            ("Volume","volume"),("iNAV","inav"),("LDCP","ldcp")]:
            m2 = re.search(label + r'\s+([\d,]+\.?\d*)', full)
            if m2: q[key] = num(m2.group(1))
        w52 = re.search(r'52[- ]WEEK RANGE.*?([\d,\.]+)\s*[—–-]+\s*([\d,\.]+)', full, re.I)
        if w52:
            q["week52_low"]  = num(w52.group(1))
            q["week52_high"] = num(w52.group(2))
        res["quote"] = q
    except Exception as e:
        print(f"  [{symbol}] quote err: {e}")
        res["quote"] = {}

    # ── ETF STATS ─────────────────────────────────────────────────────────────
    try:
        s = {}
        a = re.search(r'Fund Size.*?Rs\.\s*([\d,]+\.?\d*)', full, re.I)
        u = re.search(r'Outstanding.*?Shares.*?([\d,]+)', full, re.I)
        c = re.search(r'Market Cap.*?Rs\.\s*([\d,]+\.?\d*)', full, re.I)
        if a: s["aum"]              = num(a.group(1))
        if u: s["outstanding_units"] = num(u.group(1))
        if c: s["market_cap_000s"]   = num(c.group(1))
        res["stats"] = s
    except Exception as e:
        print(f"  [{symbol}] stats err: {e}")
        res["stats"] = {}

    # ── HOLDINGS ──────────────────────────────────────────────────────────────
    # The basket table appears AFTER the h2 "Underlying Basket Per 10,000 ETF Units"
    # heading. An h3 "Cash Component: ..." sits between heading and table — skip it.
    # The basket table's rows have <a href="/company/TICKER"> links.
    try:
        holdings   = []
        basket_tbl = None

        # Primary: anchor on heading, walk forward skipping h3
        anchor = soup.find(string=re.compile(r"Underlying Basket", re.I))
        if anchor:
            for el in anchor.find_all_next(["table", "h1", "h2"]):
                if el.name in ("h1", "h2"):
                    el_txt = el.get_text()
                    # Stop at any new major section that isn't basket-related
                    if not any(k in el_txt for k in ("Basket", "Cash")):
                        break
                if el.name == "table":
                    if el.find("a", href=re.compile(r"/company/", re.I)):
                        basket_tbl = el
                        break

        # Fallback: first table in the page that has /company/ links pointing
        # to uppercase ticker-like symbols
        if not basket_tbl:
            for tbl in soup.find_all("table"):
                links = tbl.find_all("a", href=re.compile(r"/company/", re.I))
                if not links:
                    continue
                first = links[0].get_text(strip=True).upper()
                if re.match(r"^[A-Z][A-Z0-9]{1,7}$", first):
                    basket_tbl = tbl
                    break

        if basket_tbl:
            for row in basket_tbl.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if len(cells) < 2:
                    continue
                a_tag = cells[0].find("a", href=re.compile(r"/company/", re.I))
                sym   = (a_tag.get_text(strip=True) if a_tag
                         else tx(cells[0])).upper().strip()
                # Validate: 2-8 uppercase alphanumerics, must start with a letter
                if not re.match(r"^[A-Z][A-Z0-9]{1,7}$", sym):
                    continue
                holdings.append({
                    "symbol":     sym,
                    "name":       tx(cells[1]) if len(cells) > 1 else "",
                    "shares":     num(tx(cells[2])) if len(cells) > 2 else None,
                    "price":      None,
                    "ldcp":       None,
                    "change":     None,
                    "change_pct": None,
                })

        cash_m  = re.search(r'Cash Component[:\s]*Rs\.\s*([\d,]+\.?\d*)', full, re.I)
        cpct_m  = re.search(r'%\s*Cash Component[:\s]*([\d\.]+)%', full, re.I)
        bdate_m = re.search(
            r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s*20\d\d)',
            full)

        res["holdings"]        = holdings
        res["basket_cash"]     = num(cash_m.group(1))  if cash_m  else None
        res["basket_cash_pct"] = num(cpct_m.group(1))  if cpct_m  else None
        res["basket_as_of"]    = bdate_m.group(1)       if bdate_m else None
        print(f"  [{symbol}] holdings: {len(holdings)} stocks"
              + (f"  (cash: Rs.{res['basket_cash']})" if res["basket_cash"] else ""))

    except Exception as e:
        print(f"  [{symbol}] holdings err: {e}")
        res["holdings"] = []

    # ── ANNOUNCEMENTS ─────────────────────────────────────────────────────────
    try:
        anns    = []
        ann_div = soup.find(id="announcements")
        cats    = ["Financial Results", "Board Meetings", "Others"]
        if ann_div:
            for i, tbl in enumerate(ann_div.find_all("table")):
                cat = cats[i] if i < len(cats) else "Others"
                for row in tbl.find_all("tr")[1:]:
                    cells = row.find_all(["td","th"])
                    if len(cells) < 2: continue
                    title_s = tx(cells[1])
                    if not title_s: continue
                    pdf_url = ""
                    pdf_a   = row.find("a", href=re.compile(r"\.pdf", re.I))
                    if pdf_a:
                        h = pdf_a.get("href","")
                        pdf_url = (PSX_BASE + h) if h.startswith("/") else h
                    anns.append({
                        "date":      tx(cells[0]),
                        "category":  cat,
                        "title":     title_s,
                        "pdf_url":   pdf_url,
                        "important": is_important(title_s),
                    })
        res["announcements"] = anns
        print(f"  [{symbol}] announcements: {len(anns)}")
    except Exception as e:
        print(f"  [{symbol}] ann err: {e}")
        res["announcements"] = []

    # ── PAYOUTS ───────────────────────────────────────────────────────────────
    try:
        payouts    = []
        payout_div = soup.find(id="payouts")
        if payout_div:
            for tbl in payout_div.find_all("table"):
                hdrs = [tx(th).lower() for th in tbl.find_all("th")]
                for row in tbl.find_all("tr")[1:]:
                    cells = [tx(c) for c in row.find_all(["td","th"])]
                    if not any(cells): continue
                    entry = {}
                    for j, h in enumerate(hdrs):
                        if j < len(cells): entry[h or f"col{j}"] = cells[j]
                    if entry: payouts.append(entry)
        res["payouts"] = payouts
        print(f"  [{symbol}] payouts: {len(payouts)}")
    except Exception as e:
        print(f"  [{symbol}] payouts err: {e}")
        res["payouts"] = []

    # ── RETURNS ───────────────────────────────────────────────────────────────
    try:
        rets = {}
        anch = soup.find(string=re.compile(r"ETF NAV vs Benchmark", re.I))
        if anch:
            parent = anch.find_parent()
            tbl = parent.find_next("table") if parent else None
            if tbl:
                rows = tbl.find_all("tr")
                if len(rows) >= 2:
                    hdrs = [tx(c) for c in rows[0].find_all(["th","td"])]
                    vals = [tx(c) for c in rows[1].find_all(["th","td"])]
                    for h, v in zip(hdrs, vals):
                        if h in ("1W","1M","3M","6M","YTD","1Y","3Y","5Y"):
                            rets[h] = num(v)
        res["returns"] = rets
    except Exception as e:
        print(f"  [{symbol}] returns err: {e}")
        res["returns"] = {}

    # ── PROFILE / TERMSHEET ───────────────────────────────────────────────────
    try:
        profile = {}
        for tbl in soup.find_all("table"):
            txt = tbl.get_text()
            if "Fund Name" in txt and "Management Fee" in txt:
                for row in tbl.find_all("tr"):
                    cells = row.find_all(["td","th"])
                    if len(cells) == 2:
                        k, v = tx(cells[0]).strip(), tx(cells[1]).strip()
                        if k and v: profile[k] = v
                break
        prof_div = soup.find(id="profile")
        if prof_div:
            for row in prof_div.find_all("tr"):
                cells = row.find_all(["td","th"])
                if len(cells) == 2:
                    k, v = tx(cells[0]).strip(), tx(cells[1]).strip()
                    if k and v and k not in profile: profile[k] = v
        res["profile"] = profile
    except Exception as e:
        print(f"  [{symbol}] profile err: {e}")
        res["profile"] = {}

    return res


def _get_tick(sym):
    """Fetch a single tick from psxterminal. Returns normalised dict or None."""
    try:
        r = requests.get(f"{TERM_BASE}/api/ticks/REG/{sym}",
                         headers=API_HDR, timeout=8)
        if r.status_code != 200:
            return None
        data = r.json()
        tick = data.get("data", data) if isinstance(data, dict) else {}
        if not isinstance(tick, dict):
            return None
        price = tick.get("price") or tick.get("close") or tick.get("last")
        if not price:
            return None
        # Per psx_daily_scanner.py: changePercent is a decimal fraction (0.07 = 7%)
        cpct  = float(tick.get("changePercent", tick.get("change_pct", 0)) or 0) * 100
        chg   = float(tick.get("change", 0) or 0)
        ldcp  = tick.get("ldcp") or tick.get("prevClose")
        return {
            "price":      round(float(price), 2),
            "change":     round(chg, 2),
            "change_pct": round(cpct, 2),
            "ldcp":       round(float(ldcp), 2) if ldcp else None,
            "volume":     int(tick.get("volume", 0) or 0),
            "high":       round(float(tick.get("high", 0) or 0), 2) or None,
            "low":        round(float(tick.get("low",  0) or 0), 2) or None,
            "inav":       round(float(tick.get("inav") or tick.get("iNav") or 0), 2) or None,
        }
    except Exception:
        return None


def _fix_etf_price(raw, psx_scraped):
    """
    psxterminal returns some ETF prices in paisa (100x Rs value).
    Detect by comparing with the PSX-scraped price and divide if ~100x off.
    """
    if not raw:
        return raw
    p = float(raw)
    if psx_scraped and psx_scraped > 0:
        ratio = p / float(psx_scraped)
        if 80 < ratio < 120:           # psxterminal is ~100x → in paisa
            return round(p / 100, 2)
    return round(p, 2)


def enrich_etf_quote(etf_data):
    """Enrich ETF quote with live psxterminal tick, handling paisa/rupee scale."""
    sym = etf_data.get("symbol", "")
    psx_price = etf_data.get("quote", {}).get("price")   # from PSX scrape (correct)
    tick = _get_tick(sym)
    if not tick:
        return
    q = etf_data.setdefault("quote", {})
    q["price"]  = _fix_etf_price(tick["price"],  psx_price)
    q["ldcp"]   = _fix_etf_price(tick["ldcp"],   psx_price)
    q["high"]   = _fix_etf_price(tick["high"],   psx_price)
    q["low"]    = _fix_etf_price(tick["low"],    psx_price)
    q["inav"]   = _fix_etf_price(tick["inav"],   psx_price)
    q["change_pct"] = tick["change_pct"]
    # Recalculate change from corrected price and ldcp
    if q.get("ldcp") and q.get("price"):
        q["change"] = round(q["price"] - q["ldcp"], 2)
    if tick["volume"]:
        q["volume"] = tick["volume"]
    print(f"  [{sym}] price: {q['price']}  chg: {q['change_pct']:+.2f}%  "
          f"vol: {q.get('volume',0):,}")


def enrich_prices(holdings):
    """Fetch live LDCP/price/change_pct for each holding stock from psxterminal."""
    if not holdings: return
    print(f"    Fetching prices for {len(holdings)} holdings...")
    for h in holdings:
        sym = h.get("symbol", "")
        if not sym: continue
        tick = _get_tick(sym)
        if tick:
            h["price"]      = tick["price"]
            h["change"]     = tick["change"]
            h["change_pct"] = tick["change_pct"]
            h["ldcp"]       = tick["ldcp"]
        time.sleep(0.12)


def diff_holdings(symbol, today, prior_data):
    if not prior_data:
        return {"added":[], "removed":[], "share_changes":[]}
    prev_etf = next(
        (e for e in prior_data.get("etfs",[]) if e["symbol"]==symbol), None)
    if not prev_etf:
        return {"added":[], "removed":[], "share_changes":[]}
    prev = {h["symbol"]: h for h in prev_etf.get("holdings",[])}
    curr = {h["symbol"]: h for h in today}
    added   = sorted(s for s in curr if s not in prev)
    removed = sorted(s for s in prev if s not in curr)
    changes = []
    for s in curr:
        if s in prev:
            ps, cs = prev[s].get("shares"), curr[s].get("shares")
            if ps is not None and cs is not None and ps != cs:
                changes.append({"symbol":s,"name":curr[s].get("name",""),
                                 "shares_prev":ps,"shares_curr":cs,
                                 "delta":round(cs-ps,2)})
    return {"added":added,"removed":removed,"share_changes":changes}


def fetch_indexes():
    """Fetch value + change for the 3 strip indexes (KMI30, KSE30, MII30)."""
    indexes = {}
    for idx in INDEX_SYMBOLS:
        for ep in [f"{TERM_BASE}/api/ticks/IDX/{idx}",
                   f"{TERM_BASE}/api/ticks/IDX/{idx.lower()}"]:
            try:
                r = requests.get(ep, headers=API_HDR, timeout=10)
                if r.status_code != 200: continue
                data = r.json()
                tick = data.get("data", data) if isinstance(data, dict) else {}
                val  = tick.get("price") or tick.get("close") or tick.get("last")
                chg  = float(tick.get("changePercent",
                                       tick.get("change_pct", 0)) or 0)
                if val:
                    indexes[idx] = {
                        "value":      round(float(val), 2),
                        "change_pct": round(chg*100 if abs(chg)<5 else chg, 2),
                    }
                    print(f"  [IDX] {idx}: {indexes[idx]['value']:,.2f}  "
                          f"({indexes[idx]['change_pct']:+.2f}%)")
                    break
            except Exception as e:
                print(f"  [IDX] {idx}: {e}")
    return indexes


# ── Index card definitions & hardcoded constituent lists ─────────────────────
# Constituents sourced from ksestocks.com (PSX official index composition).
# These change only at quarterly rebalancing — update as needed.
INDEX_CARD_DEFS = [
    ("KSE100", "KSE 100 Index",            "Top 100 companies by market cap",            "#0d3d6e"),
    ("KMI30",  "KMI 30 Index",             "Top 30 Shariah-compliant companies",         "#1a5e20"),
    ("KSE30",  "KSE 30 Index",             "Top 30 companies by free-float market cap",  "#4a1942"),
    ("MII30",  "Mahaana Islamic Index 30",  "Top 30 Shariah-compliant by free-float",     "#7b3800"),
]

# KSE-100 constituents (100 symbols)
KSE100_SYMS = [
    "KEL","HUBC","KAPCO","SPWL",                          # Power
    "OGDC","PPL","POL","MARI",                            # Oil & Gas Exploration
    "SCBPL","BOP","NBP","MEBL","BAFL","FABL","HBL",
    "AKBL","UBL","MCB","ABL","BAHL","HMB",               # Banks
    "PTC","TRG","SYS",                                    # Tech
    "FCCL","MLCF","DGKC","LUCK","PIOC","KOHC","CHCC",    # Cement
    "DCR",                                                # REIT
    "FATIMA","EFERT","FFBL","FFC","ENGRO",                # Fertilizer
    "PIBTL",                                              # Transport
    "LOTCHEM","EPCL","COLG","ARPL",                       # Chemical
    "UNITY","NATF","NESTLE","MUREB",                      # Food
    "HASCOL","SSGC","SNGP","PSO","SHEL","APL",           # Oil & Gas Marketing
    "ILP","GATM","ANL","FML","NML","KTML","NCL",          # Textile
    "PAEL",                                               # Cables
    "GHGL",                                               # Glass
    "PSX","OLPL",                                         # Investment
    "ISL","INIL",                                         # Engineering
    "SEARL","GLAXO","AGP","ABOT","HINOON",                # Pharma
    "AICL","EFUG","JLICL",                                # Insurance
    "IBFL",                                               # Synthetic
    "HGFA",                                               # Mutual Fund
    "PAKT","PMPK",                                        # Tobacco
    "FHAM",                                               # Modarabas
    "HCAR","MTL","ATLH","PSMC","INDU",                    # Autos
    "MLPL",                                               # Leasing
    "TELE","NETSOL","AVN","WTL",                          # IT
    "PNSC","DNFL",                                        # Transport
    "ATRL","PARCO","NRL","BYCO",                          # Refinery
    "LUCK","SAPPH","TPPL",                                # Other
    "SRVI","KFCH",                                        # Misc
]
# Remove duplicates while preserving order
_seen = set(); KSE100_SYMS = [s for s in KSE100_SYMS if not (_seen.add(s) or s in _seen)]

# KMI-30 (top 30 Shariah-compliant by market cap)
KMI30_SYMS = [
    "OGDC","PPL","LUCK","ENGRO","POL","MARI","FFC","EFERT",
    "HUBC","MCB","HBL","UBL","MEBL","NBP","BAFL","BAHL",
    "PSO","SNGP","NML","PAEL","ISL","SEARL","DGKC","FCCL",
    "TRG","MLCF","CHCC","EPCL","LOTCHEM","SSGC",
]

# KSE-30 (top 30 by free-float market cap)
KSE30_SYMS = [
    "OGDC","PPL","POL","MARI",                            # Oil exploration
    "BOP","NBP","MEBL","BAFL","HBL","UBL","MCB","BAHL",   # Banks
    "FCCL","MLCF","DGKC","LUCK","CHCC",                   # Cement
    "EFERT","FFC","ENGRO",                                 # Fertilizer
    "HUBC",                                               # Power
    "UNITY",                                              # Food
    "HASCOL","SNGP","PSO",                                # Oil marketing
    "PAEL",                                               # Cables
    "TRG",                                                # Tech
    "ISL",                                                # Engineering
    "SEARL",                                              # Pharma
    "NML",                                                # Textile
]

# MII-30 (Mahaana Islamic Index 30 — Shariah-compliant free-float)
MII30_SYMS = [
    "OGDC","PPL","LUCK","ENGRO","POL","MARI","FFC","EFERT",
    "HUBC","MCB","HBL","UBL","MEBL","BAFL","BAHL",
    "PSO","SNGP","NML","PAEL","ISL","SEARL","DGKC","FCCL",
    "TRG","MLCF","CHCC","EPCL","LOTCHEM","SSGC","HUBC",
]
_seen2 = set(); MII30_SYMS = [s for s in MII30_SYMS if not (_seen2.add(s) or s in _seen2)]

INDEX_MEMBERS = {
    "KSE100": KSE100_SYMS,
    "KMI30":  KMI30_SYMS,
    "KSE30":  KSE30_SYMS,
    "MII30":  MII30_SYMS,
}

# Company names for display (common ones — others fall back to symbol)
COMPANY_NAMES = {
    "OGDC":"Oil & Gas Dev Co","PPL":"Pakistan Petroleum","POL":"Pakistan Oilfields",
    "MARI":"Mari Petroleum","KEL":"K-Electric","HUBC":"Hub Power","KAPCO":"KAPCO",
    "BOP":"Bank of Punjab","NBP":"Natl Bank Pakistan","MEBL":"Meezan Bank",
    "BAFL":"Bank Alfalah","HBL":"Habib Bank","UBL":"United Bank","MCB":"MCB Bank",
    "ABL":"Allied Bank","BAHL":"Bank Al-Habib","FABL":"Faysal Bank","AKBL":"Askari Bank",
    "HMB":"Habib Metro Bank","SCBPL":"Standard Chartered",
    "FCCL":"Fauji Cement","MLCF":"Maple Leaf Cement","DGKC":"DG Khan Cement",
    "LUCK":"Lucky Cement","PIOC":"Pioneer Cement","KOHC":"Kohat Cement","CHCC":"Cherat Cement",
    "EFERT":"Engro Fertilizers","FFC":"Fauji Fertilizer","FFBL":"FFBL","ENGRO":"Engro Corp",
    "FATIMA":"Fatima Fertilizer","PSO":"Pakistan State Oil","SSGC":"Sui Southern Gas",
    "SNGP":"Sui Northern Gas","SHEL":"Shell Pakistan","APL":"Attock Petroleum","HASCOL":"Hascol",
    "NML":"Nishat Mills","ILP":"Interloop","GATM":"Gul Ahmed Textile","FML":"Feroze1888",
    "NCL":"Nishat Chunian","KTML":"Kohinoor Textile","ANL":"Azgard Nine",
    "PAEL":"Pak Elektron","TRG":"TRG Pakistan","SYS":"Systems Ltd","PTC":"PTCL",
    "SEARL":"The Searle Co","GLAXO":"GlaxoSmithKline","AGP":"AGP Ltd","ABOT":"Abbott Pakistan",
    "ISL":"Intl Steels","INIL":"Intl Industries","LOTCHEM":"Lotte Chemical","EPCL":"Engro Polymer",
    "COLG":"Colgate Palmolive","UNITY":"Unity Foods","NATF":"National Foods",
    "DCR":"Dolmen City REIT","PSX":"Pakistan Stock Exchange","GHGL":"Ghani Glass",
    "PAKT":"Pakistan Tobacco","PMPK":"Philip Morris","AICL":"Adamjee Insurance",
    "EFUG":"EFU General","JLICL":"Jubilee Life","PIBTL":"PIBT","IBFL":"Ibrahim Fibre",
    "HGFA":"HBL Growth Fund","HCAR":"Honda Atlas","MTL":"Millat Tractors",
    "ATLH":"Atlas Honda","ATRL":"Attock Refinery","PARCO":"PARCO","NRL":"Natl Refinery",
    "BYCO":"Byco Petroleum",
}


def fetch_index_cards():
    """
    For KSE100, KMI30, KSE30, MII30:
      1. Fetch index level from psxterminal IDX tick
      2. Fetch individual stock ticks for each hardcoded constituent
    Returns list of index card dicts.
    """
    cards = []
    for idx_sym, name, desc, color in INDEX_CARD_DEFS:
        print(f"\n  [INDEX CARD] {idx_sym}")
        card = {
            "symbol": idx_sym, "name": name, "desc": desc, "color": color,
            "value": None, "change": None, "change_pct": None,
            "high": None, "low": None, "constituents": [],
        }

        # ── Index level ───────────────────────────────────────────────────────
        for ep in [f"{TERM_BASE}/api/ticks/IDX/{idx_sym}",
                   f"{TERM_BASE}/api/ticks/IDX/{idx_sym.lower()}"]:
            try:
                r = requests.get(ep, headers=API_HDR, timeout=10)
                if r.status_code != 200: continue
                data = r.json()
                tick = data.get("data", data) if isinstance(data, dict) else {}
                val  = tick.get("price") or tick.get("close") or tick.get("last")
                if val:
                    cpct = float(tick.get("changePercent",
                                          tick.get("change_pct", 0)) or 0) * 100
                    card["value"]      = round(float(val), 2)
                    card["change"]     = round(float(tick.get("change", 0) or 0), 2)
                    card["change_pct"] = round(cpct, 2)
                    card["high"]  = round(float(tick.get("high", 0) or 0), 2) or None
                    card["low"]   = round(float(tick.get("low",  0) or 0), 2) or None
                    print(f"    value: {card['value']:,.2f}  ({card['change_pct']:+.2f}%)")
                    break
            except Exception as e:
                print(f"    IDX tick err: {e}")

        # ── Fetch tick for each constituent symbol ────────────────────────────
        syms = INDEX_MEMBERS.get(idx_sym, [])
        print(f"    Fetching {len(syms)} constituent ticks...")
        constituents = []
        for i, sym in enumerate(syms):
            tick = _get_tick(sym)
            if tick:
                constituents.append({
                    "symbol":     sym,
                    "name":       COMPANY_NAMES.get(sym, sym),
                    "price":      tick["price"],
                    "ldcp":       tick["ldcp"],
                    "change":     tick["change"],
                    "change_pct": tick["change_pct"],
                    "volume":     tick["volume"],
                })
            else:
                constituents.append({
                    "symbol": sym, "name": COMPANY_NAMES.get(sym, sym),
                    "price": None, "ldcp": None,
                    "change": 0, "change_pct": 0, "volume": None,
                })
            if (i+1) % 20 == 0:
                print(f"      {i+1}/{len(syms)} done")
            time.sleep(0.15)

        found = sum(1 for c in constituents if c["price"])
        print(f"    ✓ {found}/{len(syms)} prices fetched")
        constituents.sort(key=lambda x: x.get("change_pct") or 0)
        card["constituents"] = constituents
        cards.append(card)
        time.sleep(1)

    return cards


def load_prior():
    idx_path = os.path.join(OUT_DIR, "index.json")
    if not os.path.exists(idx_path): return None
    try:
        dates = json.load(open(idx_path))
        dates.sort(reverse=True)
        today = datetime.now().strftime("%Y-%m-%d")
        for d in dates:
            if d >= today: continue
            p = os.path.join(OUT_DIR, f"{d}.json")
            if os.path.exists(p): return json.load(open(p))
    except Exception:
        pass
    return None


def save(date_str, data):
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, f"{date_str}.json")
    with open(path, "w") as f:
        json.dump(data, f, separators=(",",":"))
    print(f"\n[SAVE] {path}  ({os.path.getsize(path)//1024} KB)")
    idx_path = os.path.join(OUT_DIR, "index.json")
    dates    = json.load(open(idx_path)) if os.path.exists(idx_path) else []
    if date_str not in dates: dates.append(date_str)
    dates.sort(reverse=True)
    with open(idx_path, "w") as f: json.dump(dates, f)
    print(f"[INDEX] {len(dates)} dates on record")


def main():
    date_str = datetime.now().strftime("%Y-%m-%d")
    print(f"\n{'='*64}\n  PSX ETF Monitor v2 — {date_str}\n{'='*64}\n")

    prior = load_prior()
    etfs  = []

    for i, sym in enumerate(ETF_SYMBOLS):
        print(f"\n[{i+1}/{len(ETF_SYMBOLS)}] {sym}")
        etf = scrape_etf(sym)
        if etf:
            etf["holdings_diff"] = diff_holdings(sym, etf.get("holdings",[]), prior)
            enrich_etf_quote(etf)          # ← fix ETF price scale (paisa→Rs)
            enrich_prices(etf.get("holdings",[]))
            dif = etf["holdings_diff"]
            if dif["added"] or dif["removed"]:
                print(f"  ⚠  HOLDINGS CHANGED  +{dif['added']}  -{dif['removed']}")
            etfs.append(etf)
        time.sleep(2)

    print("\n[INDEXES — strip values]")
    indexes = fetch_indexes()

    print("\n[INDEX CARDS — KSE100 / KMI30 / KSE30 / MII30]")
    index_cards = fetch_index_cards()

    save(date_str, {
        "date":        date_str,
        "etfs":        etfs,
        "indexes":     indexes,
        "index_cards": index_cards,
    })

    print(f"\n{'─'*64}")
    for e in etfs:
        q   = e.get("quote",{})
        dif = e.get("holdings_diff",{})
        chg_flag = " ⚠ CHANGED" if (dif.get("added") or dif.get("removed")) else ""
        print(f"  {e['symbol']:10}  price:{q.get('price','?'):>7}  "
              f"chg:{q.get('change_pct','?')}%  "
              f"h:{len(e.get('holdings',[]))}  "
              f"ann:{len(e.get('announcements',[]))}{chg_flag}")
    print(f"{'─'*64}\n")


if __name__ == "__main__":
    main()
