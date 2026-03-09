"""
PSX ETF Monitor — Scraper v1
=====================================================================
Source: dps.psx.com.pk  (official PSX data portal — fully rendered HTML)

Saves to:
  etf_data/YYYY-MM-DD.json   — one file per trading day
  etf_data/index.json        — sorted list of all available dates

Per ETF collected:
  quote        : price, change, change_pct, open, high, low, volume,
                 inav, ldcp, week52_high, week52_low, market_cap, aum,
                 outstanding_units
  holdings     : [{symbol, name, shares}] — basket per 10,000 units
  holdings_diff: {added, removed, share_changes} vs prior day
  announcements: [{date, category, title, pdf_url, important}]
  payouts      : [{date, type, rate, book_close, payment_date}]
  returns      : {1W, 1M, 3M, 6M, 1Y} — parsed from returns table
  profile      : {fund_name, manager, benchmark, launch, fee, risk, ...}

Indexes via psxterminal: KMI30, KSE30, MII30
Schedule: 5:30 PM PKT = 12:30 UTC, Mon-Fri
=====================================================================
"""

import requests, json, re, time, os, sys
from datetime import datetime
from bs4 import BeautifulSoup

PSX_BASE   = "https://dps.psx.com.pk"
TERM_BASE  = "https://psxterminal.com"
OUTPUT_DIR = "etf_data"

SCRAPE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://dps.psx.com.pk/",
}

API_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
    "Origin":  TERM_BASE,
    "Referer": TERM_BASE + "/",
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

INDEX_SYMBOLS = ["KMI30", "KSE30", "MII30"]

IMPORTANCE_KEYWORDS = [
    "dividend", "interim dividend", "rebalancing", "book closure",
    "financial result", "annual result", "bonus", "right shares",
    "fund manager report", "credit of", "completion of rebalancing",
]


# ── Helpers ───────────────────────────────────────────────────────────────────
def txt(el):
    return " ".join(el.get_text().split()) if el else ""

def num(s):
    if not s: return None
    cleaned = re.sub(r"[Rs.,\s%]", "", str(s))
    try:    return float(cleaned)
    except: return None

def is_important(title):
    tl = title.lower()
    return any(kw in tl for kw in IMPORTANCE_KEYWORDS)


# ── Scrape one ETF ────────────────────────────────────────────────────────────
def scrape_etf(symbol):
    url = f"{PSX_BASE}/etf/{symbol}"
    print(f"  [{symbol}] fetching {url}")
    try:
        r = requests.get(url, headers=SCRAPE_HEADERS, timeout=25)
        r.raise_for_status()
    except Exception as e:
        print(f"  [{symbol}] ❌ fetch failed: {e}")
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    page = r.text
    result = {
        "symbol": symbol,
        "name":   ETF_FULL_NAMES.get(symbol, symbol),
    }

    # ── QUOTE ─────────────────────────────────────────────────────────────────
    try:
        q = {}

        # Price line: "Rs.10.82"
        price_m = re.search(r'Rs\.\s*([\d,]+\.?\d*)', page)
        if price_m: q["price"] = num(price_m.group(1))

        # Change line: "-0.15\n(-1.37%)" — look for both together
        chg_m = re.search(r'([-+]?\d+\.\d+)\s*\n\s*\(([-+]?\d+\.\d+)%\)', page)
        if chg_m:
            q["change"]     = num(chg_m.group(1))
            q["change_pct"] = num(chg_m.group(2))

        # Parse the key-value table rows in the QUOTE section
        quote_div = soup.find(id="quote")
        if not quote_div:
            # fallback: first table-like section
            quote_div = soup

        def extract_kv(container):
            kv = {}
            rows = container.find_all("tr") if container else []
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    k = txt(cells[0]).lower().strip().replace(" ", "_")
                    v = txt(cells[1])
                    if k and v: kv[k] = v
            return kv

        kv = extract_kv(quote_div if quote_div != soup else soup)

        # Direct key-value parsing from full page text for common fields
        full = soup.get_text(" ")
        for label, key in [
            ("Open",    "open"),    ("High",   "high"),
            ("Low",     "low"),     ("Volume", "volume"),
            ("iNAV",    "inav"),    ("LDCP",   "ldcp"),
        ]:
            m = re.search(label + r'\s+([\d,]+\.?\d*)', full)
            if m: q[key] = num(m.group(1))

        # 52-week range: "9.57 — 14.70"
        w52 = re.search(r'52[- ]WEEK RANGE.*?([\d,\.]+)\s*[—–-]+\s*([\d,\.]+)', full, re.I)
        if w52:
            q["week52_low"]  = num(w52.group(1))
            q["week52_high"] = num(w52.group(2))

        result["quote"] = q
    except Exception as e:
        print(f"  [{symbol}] quote parse error: {e}")
        result["quote"] = {}

    # ── ETF STATS (AUM, Units, Market Cap) ────────────────────────────────────
    try:
        stats = {}
        full = soup.get_text(" ")

        aum_m   = re.search(r'Fund Size.*?Rs\.\s*([\d,]+\.?\d*)', full, re.I)
        units_m = re.search(r'Outstanding Shares.*?([\d,]+)', full, re.I)
        mcap_m  = re.search(r'Market Cap.*?Rs\.\s*([\d,]+\.?\d*)', full, re.I)

        if aum_m:   stats["aum"]               = num(aum_m.group(1))
        if units_m: stats["outstanding_units"]  = num(units_m.group(1))
        if mcap_m:  stats["market_cap_000s"]    = num(mcap_m.group(1))

        result["stats"] = stats
    except Exception as e:
        print(f"  [{symbol}] stats parse error: {e}")
        result["stats"] = {}

    # ── HOLDINGS (Underlying Basket table) ────────────────────────────────────
    try:
        holdings = []

        # Find the basket section header
        basket_hdr = soup.find(string=re.compile("Underlying Basket", re.I))
        basket_tbl = None
        if basket_hdr:
            parent = basket_hdr.find_parent()
            while parent:
                tbl = parent.find_next("table")
                if tbl:
                    basket_tbl = tbl
                    break
                parent = parent.find_parent()

        # Fallback: find any table with Symbol/Name/Shares columns
        if not basket_tbl:
            for tbl in soup.find_all("table"):
                hdrs = [txt(th).lower() for th in tbl.find_all("th")]
                if "shares" in hdrs and ("symbol" in hdrs or "name" in hdrs):
                    basket_tbl = tbl
                    break

        if basket_tbl:
            for row in basket_tbl.find_all("tr")[1:]:  # skip header
                cells = row.find_all(["td", "th"])
                if len(cells) < 2: continue
                # Symbol may be a link
                a = cells[0].find("a")
                symbol_val = (a.get_text(strip=True) if a else txt(cells[0]))
                name_val   = txt(cells[1]) if len(cells) > 1 else ""
                shares_val = num(txt(cells[2])) if len(cells) > 2 else None
                if symbol_val and symbol_val.upper() not in ("SYMBOL", ""):
                    holdings.append({
                        "symbol": symbol_val.upper(),
                        "name":   name_val,
                        "shares": shares_val,
                    })

        # Also extract cash component
        cash_m = re.search(r'Cash Component.*?Rs\.\s*([\d,]+\.?\d*)', soup.get_text(" "), re.I)
        cash_pct_m = re.search(r'%\s*Cash Component.*?([\d\.]+)%', soup.get_text(" "), re.I)
        basket_date_m = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d+,\s*20\d\d',
                                   soup.get_text(" "))

        result["holdings"] = holdings
        result["basket_cash"]       = num(cash_m.group(1)) if cash_m else None
        result["basket_cash_pct"]   = num(cash_pct_m.group(1)) if cash_pct_m else None
        result["basket_as_of"]      = basket_date_m.group(0) if basket_date_m else None
        print(f"  [{symbol}] holdings: {len(holdings)}")

    except Exception as e:
        print(f"  [{symbol}] holdings parse error: {e}")
        result["holdings"] = []

    # ── ANNOUNCEMENTS ─────────────────────────────────────────────────────────
    try:
        announcements = []
        ann_div = soup.find(id="announcements")
        if ann_div:
            # Three tab sections: Financial Results, Board Meetings, Others
            tab_names = ["Financial Results", "Board Meetings", "Others"]
            tables = ann_div.find_all("table")
            for i, tbl in enumerate(tables):
                cat = tab_names[i] if i < len(tab_names) else "Others"
                for row in tbl.find_all("tr")[1:]:
                    cells = row.find_all(["td", "th"])
                    if len(cells) < 2: continue
                    date_s  = txt(cells[0])
                    title_s = txt(cells[1])
                    if not title_s: continue
                    # PDF link
                    pdf_url = ""
                    pdf_a = row.find("a", href=re.compile(r"\.pdf", re.I))
                    if pdf_a:
                        href = pdf_a.get("href", "")
                        pdf_url = (PSX_BASE + href) if href.startswith("/") else href

                    announcements.append({
                        "date":      date_s,
                        "category":  cat,
                        "title":     title_s,
                        "pdf_url":   pdf_url,
                        "important": is_important(title_s),
                    })

        result["announcements"] = announcements
        print(f"  [{symbol}] announcements: {len(announcements)}")

    except Exception as e:
        print(f"  [{symbol}] announcements parse error: {e}")
        result["announcements"] = []

    # ── PAYOUTS ───────────────────────────────────────────────────────────────
    try:
        payouts = []
        payout_div = soup.find(id="payouts")
        if payout_div:
            for tbl in payout_div.find_all("table"):
                hdrs = [txt(th).lower() for th in tbl.find_all("th")]
                for row in tbl.find_all("tr")[1:]:
                    cells = [txt(c) for c in row.find_all(["td", "th"])]
                    if not any(cells): continue
                    entry = {}
                    for j, hdr in enumerate(hdrs):
                        if j < len(cells):
                            entry[hdr or f"col{j}"] = cells[j]
                    if entry: payouts.append(entry)

        result["payouts"] = payouts
        print(f"  [{symbol}] payouts: {len(payouts)}")

    except Exception as e:
        print(f"  [{symbol}] payouts parse error: {e}")
        result["payouts"] = []

    # ── RETURNS ───────────────────────────────────────────────────────────────
    try:
        # The returns section has NAV vs Benchmark for 1W 1M 6M YTD 1Y 3Y 5Y
        # These are JS-rendered so values may not be in HTML.
        # We extract whatever is present and mark nulls for the frontend.
        returns = {}
        # Try to find any return table rows
        rets_section = soup.find(string=re.compile("ETF NAV vs Benchmark", re.I))
        if rets_section:
            parent = rets_section.find_parent()
            tbl = parent.find_next("table") if parent else None
            if tbl:
                rows = tbl.find_all("tr")
                if len(rows) >= 2:
                    hdrs  = [txt(th) for th in rows[0].find_all(["th","td"])]
                    vals  = [txt(td) for td in rows[1].find_all(["th","td"])]
                    for h, v in zip(hdrs, vals):
                        if h in ("1W","1M","3M","6M","YTD","1Y","3Y","5Y"):
                            returns[h] = num(v)

        result["returns"] = returns

    except Exception as e:
        print(f"  [{symbol}] returns parse error: {e}")
        result["returns"] = {}

    # ── PROFILE / TERMSHEET ───────────────────────────────────────────────────
    try:
        profile = {}
        # Termsheet is a 2-column table (key | value)
        term_hdr = soup.find(string=re.compile("Termsheet|Term Sheet|TERMSHEET", re.I))
        term_tbl = None
        if term_hdr:
            p = term_hdr.find_parent()
            while p:
                term_tbl = p.find_next("table")
                if term_tbl: break
                p = p.find_parent()

        if not term_tbl:
            # Fallback: look for any table with "Fund Name" row
            for tbl in soup.find_all("table"):
                txt_all = tbl.get_text()
                if "Fund Name" in txt_all and "Management Fee" in txt_all:
                    term_tbl = tbl
                    break

        if term_tbl:
            for row in term_tbl.find_all("tr"):
                cells = row.find_all(["td","th"])
                if len(cells) == 2:
                    k = txt(cells[0]).strip()
                    v = txt(cells[1]).strip()
                    if k and v:
                        profile[k] = v

        # Key people from profile div
        prof_div = soup.find(id="profile")
        if prof_div:
            # Address, website, auditor etc.
            for row in prof_div.find_all("tr"):
                cells = row.find_all(["td","th"])
                if len(cells) == 2:
                    k = txt(cells[0]).strip()
                    v = txt(cells[1]).strip()
                    if k and v and k not in profile:
                        profile[k] = v

        result["profile"] = profile

    except Exception as e:
        print(f"  [{symbol}] profile parse error: {e}")
        result["profile"] = {}

    return result


# ── Detect holdings changes ───────────────────────────────────────────────────
def diff_holdings(symbol, today_holdings, prior_data):
    if not prior_data:
        return {"added": [], "removed": [], "share_changes": []}

    prior_etf = next(
        (e for e in prior_data.get("etfs", []) if e["symbol"] == symbol), None
    )
    if not prior_etf:
        return {"added": [], "removed": [], "share_changes": []}

    prev = {h["symbol"]: h for h in prior_etf.get("holdings", [])}
    curr = {h["symbol"]: h for h in today_holdings}

    added   = sorted(s for s in curr if s not in prev)
    removed = sorted(s for s in prev if s not in curr)
    share_changes = []
    for s in curr:
        if s in prev:
            ps = prev[s].get("shares")
            cs = curr[s].get("shares")
            if ps is not None and cs is not None and ps != cs:
                share_changes.append({
                    "symbol":       s,
                    "name":         curr[s].get("name", ""),
                    "shares_prev":  ps,
                    "shares_curr":  cs,
                    "delta":        round(cs - ps, 2),
                })

    return {"added": added, "removed": removed, "share_changes": share_changes}


# ── Fetch index data ──────────────────────────────────────────────────────────
def fetch_indexes():
    indexes = {}
    for idx in INDEX_SYMBOLS:
        for endpoint in [
            f"{TERM_BASE}/api/ticks/IDX/{idx}",
            f"{TERM_BASE}/api/ticks/IDX/{idx.lower()}",
        ]:
            try:
                r = requests.get(endpoint, headers=API_HEADERS, timeout=10)
                if r.status_code != 200: continue
                data = r.json()
                tick = data.get("data", data) if isinstance(data, dict) else {}
                val  = tick.get("price") or tick.get("close") or tick.get("last")
                chg  = float(tick.get("changePercent", tick.get("change_pct", 0)) or 0)
                if val:
                    chg_norm = chg * 100 if abs(chg) < 5 else chg
                    indexes[idx] = {
                        "value":      round(float(val), 2),
                        "change_pct": round(chg_norm, 2),
                    }
                    print(f"  [IDX] {idx}: {indexes[idx]['value']:,.2f}  ({indexes[idx]['change_pct']:+.2f}%)")
                    break
            except Exception as e:
                print(f"  [IDX] {idx} error: {e}")
    return indexes


# ── Load prior day's data for diff ───────────────────────────────────────────
def load_prior():
    idx_path = os.path.join(OUTPUT_DIR, "index.json")
    if not os.path.exists(idx_path): return None
    try:
        dates = json.load(open(idx_path))
        dates.sort(reverse=True)
        for d in dates[1:]:          # skip today if already written
            p = os.path.join(OUTPUT_DIR, f"{d}.json")
            if os.path.exists(p):
                return json.load(open(p))
    except: pass
    return None


# ── Persist ───────────────────────────────────────────────────────────────────
def save(date_str, data):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f"{date_str}.json")
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    print(f"[SAVE] {path}  ({os.path.getsize(path)//1024} KB)")

    idx_path = os.path.join(OUTPUT_DIR, "index.json")
    dates    = json.load(open(idx_path)) if os.path.exists(idx_path) else []
    if date_str not in dates: dates.append(date_str)
    dates.sort(reverse=True)
    with open(idx_path, "w") as f: json.dump(dates, f)
    print(f"[INDEX] {len(dates)} dates on record")


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    date_str = datetime.now().strftime("%Y-%m-%d")
    print(f"\n{'='*64}\n  PSX ETF Monitor — {date_str}\n{'='*64}\n")

    prior = load_prior()
    etfs  = []

    for i, sym in enumerate(ETF_SYMBOLS):
        print(f"\n[{i+1}/{len(ETF_SYMBOLS)}] {sym}")
        etf = scrape_etf(sym)
        if etf:
            etf["holdings_diff"] = diff_holdings(sym, etf.get("holdings", []), prior)
            if etf["holdings_diff"]["added"] or etf["holdings_diff"]["removed"]:
                print(f"  [{sym}] ⚠  Holdings changed! "
                      f"added:{etf['holdings_diff']['added']}  "
                      f"removed:{etf['holdings_diff']['removed']}")
            etfs.append(etf)
        time.sleep(2)   # polite delay

    print("\n[INDEXES]")
    indexes = fetch_indexes()

    save(date_str, {"date": date_str, "etfs": etfs, "indexes": indexes})

    print(f"\n{'─'*64}")
    for e in etfs:
        q   = e.get("quote", {})
        dif = e.get("holdings_diff", {})
        chg_flag = " ⚠ CHANGED" if (dif.get("added") or dif.get("removed")) else ""
        print(f"  {e['symbol']:10}  {q.get('price','?'):>6}  "
              f"({q.get('change_pct','?')}%)  "
              f"vol:{q.get('volume','?')}  h:{len(e.get('holdings',[]))}"
              f"  ann:{len(e.get('announcements',[]))}{chg_flag}")
    print(f"{'─'*64}\n")


if __name__ == "__main__":
    main()
