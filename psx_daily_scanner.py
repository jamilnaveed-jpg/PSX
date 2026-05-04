"""
PSX Daily Scanner — yfinance Edition
=====================================================================
Data source: Yahoo Finance (PSX symbols use .KA suffix)
Symbols    : fetched from psxterminal /api/symbols (that endpoint
             still works from GitHub Actions IPs)
Fallback   : built-in symbol list if API unreachable

LISTS:
  list1 — Momentum Gainers  : change >= +4%,  volume >= 100K
  list2 — High-Volume Movers: volume >= 9M    (any direction)
  list3 — Tight Range Watch : |change| <= 0.40%
  list4 — Top Losers        : change <= -4%,  volume >= 100K
  market — breadth + per-sector breakdown
=====================================================================
"""

import yfinance as yf
import requests, json, time, os, sys, warnings, math
from datetime import datetime, date, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

warnings.filterwarnings("ignore")

# ── Config ────────────────────────────────────────────────────────
GAINER_PCT      = 4.0
LOSER_PCT       = -4.0
MIN_VOLUME_L1   = 100_000
MIN_VOLUME_L2   = 9_000_000
TIGHT_RANGE_PCT = 0.40
OUTPUT_DIR      = "daily_data"
BASE_URL        = "https://psxterminal.com"

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin":          "https://psxterminal.com",
    "Referer":         "https://psxterminal.com/",
}

# ── Built-in sector map (~120 major PSX stocks) ───────────────────
SECTOR_MAP = {
    # Banking
    "HBL":"Banking","MCB":"Banking","UBL":"Banking","ABL":"Banking",
    "NBP":"Banking","BAHL":"Banking","BAFL":"Banking","MEBL":"Banking",
    "AKBL":"Banking","BOK":"Banking","BOP":"Banking","SILK":"Banking",
    "JSBL":"Banking","SNBL":"Banking","FAYS":"Banking",
    # Cement
    "LUCK":"Cement","DGKC":"Cement","MLCF":"Cement","KOHC":"Cement",
    "FCCL":"Cement","CHCC":"Cement","PIOC":"Cement","FCEL":"Cement",
    "GWLC":"Cement","ACPL":"Cement","BWCL":"Cement","THCCL":"Cement","FLYNG":"Cement",
    # Oil & Gas
    "OGDC":"Oil & Gas","PPL":"Oil & Gas","POL":"Oil & Gas","MARI":"Oil & Gas",
    "PARCO":"Oil & Gas","PRL":"Oil & Gas","NRL":"Oil & Gas","APL":"Oil & Gas",
    "ATRL":"Oil & Gas","BYCO":"Oil & Gas","CNERGY":"Oil & Gas",
    # Fertilizer
    "ENGRO":"Fertilizer","FFC":"Fertilizer","FFBL":"Fertilizer",
    "FATIMA":"Fertilizer","EFERT":"Fertilizer",
    # Power
    "HUBC":"Power","KAPCO":"Power","NCPL":"Power","PKGP":"Power",
    "SPWL":"Power","EPQL":"Power","LALPIR":"Power","JPGL":"Power",
    "TPLP":"Power","KEL":"Power","LTPL":"Power","ATBA":"Power",
    # Textile
    "NCL":"Textile","NML":"Textile","GATM":"Textile","KTML":"Textile",
    "CRTM":"Textile","GTHM":"Textile","TREET":"Textile","KOHAT":"Textile",
    "ILP":"Textile","DHTX":"Textile","EPCL":"Textile",
    # Pharma
    "SEARL":"Pharma","FEROZ":"Pharma","GLAXO":"Pharma","ABOT":"Pharma",
    "HINOON":"Pharma","IBFL":"Pharma","SHEZ":"Pharma","PKGS":"Pharma",
    # Automobile
    "PSMC":"Automobile","INDU":"Automobile","HCAR":"Automobile","ATLH":"Automobile",
    "MTL":"Automobile","GHNL":"Automobile","SAZEW":"Automobile","RAVI":"Automobile","GHNI":"Automobile",
    # Technology
    "SYS":"Technology","TRG":"Technology","NETSOL":"Technology","AVN":"Technology","PMRS":"Technology",
    # Steel & Metals
    "ISL":"Steel","ASTL":"Steel","AGHA":"Steel","MUGHAL":"Steel","INIL":"Steel",
    # Food & Beverages
    "UNITY":"Food","NESTLE":"Food","QUICE":"Food","SHFA":"Food",
    "HPUN":"Food","COLG":"Food","IBLHL":"Food",
    # Insurance
    "AICL":"Insurance","JUBILEE":"Insurance","ADAMJEE":"Insurance","EFU":"Insurance",
    "IGIL":"Insurance","PAKRI":"Insurance","SAFL":"Insurance","NICL":"Insurance",
    # Chemicals
    "ICI":"Chemicals","BOC":"Chemicals","LOTPTA":"Chemicals","NRSL":"Chemicals",
    # Telecom
    "PTC":"Telecom",
    # Property & REITs
    "AREIT":"Property","SAIF":"Property","DOLMEN":"Property","EMCO":"Property",
}

FALLBACK_SYMBOLS = list(SECTOR_MAP.keys())


# ── Requests session ──────────────────────────────────────────────
def make_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    retry = Retry(total=3, backoff_factor=2,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://",  HTTPAdapter(max_retries=retry))
    return session

SESSION = make_session()


# ── Step 1: Get symbol list ───────────────────────────────────────
def get_symbols():
    """Fetch all PSX symbols from psxterminal (/api/symbols still works)."""
    try:
        r = SESSION.get(f"{BASE_URL}/api/symbols", timeout=20)
        if r.status_code == 200:
            data = r.json()
            raw  = data.get("data", data) if isinstance(data, dict) else data
            syms = [str(s).strip() for s in raw if s]
            if syms:
                print(f"[SYMBOLS] {len(syms)} symbols fetched from psxterminal")
                return syms
    except Exception as e:
        print(f"[SYMBOLS] psxterminal unavailable ({type(e).__name__}) — using built-in list")
    print(f"[SYMBOLS] Using built-in list of {len(FALLBACK_SYMBOLS)} symbols")
    return FALLBACK_SYMBOLS


# ── Step 2: Sector map ────────────────────────────────────────────
def get_sector_map(symbols):
    sm    = {sym: SECTOR_MAP.get(sym, "Unknown") for sym in symbols}
    known = sum(1 for v in sm.values() if v != "Unknown")
    print(f"[SECTORS] {known}/{len(symbols)} symbols mapped to sectors")
    return sm


# ── Step 3: Fetch via Yahoo Finance ──────────────────────────────
def fetch_ticks_yfinance(symbols):
    """Download EOD data for all symbols from Yahoo Finance (.KA suffix)."""
    ka_syms   = [f"{s}.KA" for s in symbols]
    ka_to_sym = {f"{s}.KA": s for s in symbols}

    print(f"[TICKS] Downloading {len(ka_syms)} ticks via Yahoo Finance...")

    # 5-day window guarantees a prev-close even across long weekends
    data = yf.download(
        ka_syms,
        period="5d",
        interval="1d",
        progress=False,
        auto_adjust=True,
        threads=True,
    )

    if data.empty:
        print("[TICKS] Yahoo Finance returned empty data")
        return []

    close  = data["Close"].dropna(how="all")
    volume = data["Volume"].dropna(how="all")
    high   = data["High"].dropna(how="all")
    low    = data["Low"].dropna(how="all")

    if len(close) < 2:
        print("[TICKS] Only one row of data — cannot compute change (market closed?)")
        return []

    today_row = close.iloc[-1]
    prev_row  = close.iloc[-2]
    vol_row   = volume.iloc[-1]
    high_row  = high.iloc[-1]
    low_row   = low.iloc[-1]

    stocks = []
    for ka_sym in ka_syms:
        if ka_sym not in close.columns:
            continue
        try:
            price = float(today_row[ka_sym])
            prev  = float(prev_row[ka_sym])
            vol   = float(vol_row[ka_sym])
            hi    = float(high_row[ka_sym])
            lo    = float(low_row[ka_sym])
        except (KeyError, TypeError, ValueError):
            continue

        if any(math.isnan(v) for v in [price, prev, vol]):
            continue
        if price <= 0 or prev <= 0:
            continue

        change  = round(price - prev, 2)
        chg_pct = round((price - prev) / prev * 100, 2)
        stocks.append({
            "symbol":     ka_to_sym[ka_sym],
            "sector":     "—",
            "price":      round(price, 2),
            "change":     change,
            "change_pct": chg_pct,
            "volume":     int(vol),
            "high":       round(hi, 2) if not math.isnan(hi) else round(price, 2),
            "low":        round(lo, 2) if not math.isnan(lo) else round(price, 2),
        })

    print(f"[TICKS] Complete — {len(stocks)}/{len(symbols)} stocks with valid data")
    return stocks


# ── Step 4: Market summary ────────────────────────────────────────
def build_market_summary(stocks):
    gainers   = sum(1 for s in stocks if s["change_pct"] > 0)
    losers    = sum(1 for s in stocks if s["change_pct"] < 0)
    unchanged = len(stocks) - gainers - losers
    total_vol = sum(s["volume"] for s in stocks)
    total_val = sum(s["price"] * s["volume"] for s in stocks)
    sectors   = {}
    for s in stocks:
        sec = s["sector"] if s["sector"] not in ("—", "", None, "Unknown") else "Unknown"
        if sec not in sectors:
            sectors[sec] = {"stocks": [], "total_vol": 0, "total_val": 0.0,
                            "gainers": 0, "losers": 0, "unchanged": 0, "wtd_pct": 0.0}
        d = sectors[sec]
        d["stocks"].append(s); d["total_vol"] += s["volume"]
        d["total_val"] += s["price"] * s["volume"]
        d["wtd_pct"]   += s["change_pct"] * s["volume"]
        if   s["change_pct"] > 0: d["gainers"]   += 1
        elif s["change_pct"] < 0: d["losers"]    += 1
        else:                     d["unchanged"] += 1
    sector_list = []
    for name, d in sectors.items():
        avg_pct = round(d["wtd_pct"] / d["total_vol"], 2) if d["total_vol"] > 0 else 0.0
        top3 = sorted(d["stocks"], key=lambda x: x["volume"], reverse=True)[:3]
        sector_list.append({
            "name": name, "total_vol": d["total_vol"],
            "total_val": round(d["total_val"], 0), "gainers": d["gainers"],
            "losers": d["losers"], "unchanged": d["unchanged"],
            "count": len(d["stocks"]), "avg_pct": avg_pct,
            "top3": [{"symbol": t["symbol"], "price": t["price"],
                      "change_pct": t["change_pct"], "volume": t["volume"]} for t in top3],
        })
    sector_list.sort(key=lambda x: x["total_vol"], reverse=True)
    return {"gainers": gainers, "losers": losers, "unchanged": unchanged,
            "total": len(stocks), "total_vol": total_vol,
            "total_val": round(total_val, 0), "sectors": sector_list}


# ── Step 5: Scanner lists ─────────────────────────────────────────
def build_lists(stocks):
    l1, l2, l3, l4 = [], [], [], []
    for s in stocks:
        p, v = s["change_pct"], s["volume"]
        if p >= GAINER_PCT and v >= MIN_VOLUME_L1: l1.append(s)
        if v >= MIN_VOLUME_L2:                     l2.append(s)
        if abs(p) <= TIGHT_RANGE_PCT:              l3.append(s)
        if p <= LOSER_PCT  and v >= MIN_VOLUME_L1: l4.append(s)
    l1.sort(key=lambda x: x["change_pct"], reverse=True)
    l2.sort(key=lambda x: x["volume"],     reverse=True)
    l3.sort(key=lambda x: abs(x["change_pct"]))
    l4.sort(key=lambda x: x["change_pct"])
    return l1, l2, l3, l4


# ── Step 6: Save ──────────────────────────────────────────────────
def save_json(date_str, data):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f"{date_str}.json")
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    print(f"[SAVE] {path} ({os.path.getsize(path) // 1024} KB)")
    idx_path = os.path.join(OUTPUT_DIR, "index.json")
    dates = json.load(open(idx_path)) if os.path.exists(idx_path) else []
    if date_str not in dates:
        dates.append(date_str)
    dates.sort(reverse=True)
    with open(idx_path, "w") as f:
        json.dump(dates, f)
    print(f"[INDEX] {len(dates)} dates on record")


# ── Main ──────────────────────────────────────────────────────────
def main():
    date_str = datetime.now().strftime("%Y-%m-%d")
    print(f"\n{'='*62}\n  PSX Daily Scanner v10 (yfinance) — {date_str}\n{'='*62}\n")

    symbols = get_symbols()
    if not symbols:
        print("[MAIN] No symbols — exiting cleanly.")
        sys.exit(0)

    sm     = get_sector_map(symbols)
    stocks = fetch_ticks_yfinance(symbols)

    for s in stocks:
        s["sector"] = sm.get(s["symbol"], "Unknown")

    if not stocks:
        print("[MAIN] No data — market is likely closed (holiday/weekend).")
        print("[MAIN] Exiting cleanly (exit code 0).")
        sys.exit(0)

    market          = build_market_summary(stocks)
    l1, l2, l3, l4 = build_lists(stocks)
    save_json(date_str, {"date": date_str, "market": market,
                         "list1": l1, "list2": l2, "list3": l3, "list4": l4})
    print(f"\n  ▲{market['gainers']} ▼{market['losers']} —{market['unchanged']} | "
          f"L1:{len(l1)} L2:{len(l2)} L3:{len(l3)} L4:{len(l4)}\n")

if __name__ == "__main__":
    main()
