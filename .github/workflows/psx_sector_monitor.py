"""
PSX Sector Monitor — Daily Data Collector
===========================================
Fetches live sector data from psxterminal.com 2 hours after PSX close.

PSX Market Hours: 9:30 AM – 3:30 PM PKT (Mon–Fri)
This script runs at: 5:30 PM PKT = 12:30 UTC

Outputs:
  sectors_data/YYYY-MM-DD.json  — one file per trading day
  sectors_data/index.json       — full history array (all dates, latest last)

GitHub Actions triggers this at 12:30 UTC Mon–Fri automatically.
Run manually:  python psx_sector_monitor.py [--date YYYY-MM-DD] [--demo]
"""

import requests, json, os, sys, time, argparse
from datetime import date, datetime, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL   = "https://psxterminal.com"
OUTPUT_DIR = "sectors_data"
MAX_HISTORY_DAYS = 365 * 2   # keep 2 years of daily files in index

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "application/json, */*",
    "Origin":     "https://psxterminal.com",
    "Referer":    "https://psxterminal.com/",
}

# ── PSX Sector Definitions ────────────────────────────────────────────────────
# Each sector has: id, name, abbreviation, and list of PSX ticker symbols
SECTORS = [
    {
        "id": "cement",
        "name": "Cement",
        "abbr": "CEM",
        "color": "#e8934a",
        "stocks": ["LUCK","DGKC","MLCF","KOHC","FCCL","CHCC","PIOC","FCEL","GWLC","ACPL","BWCL","THCCL","FLYNG"],
    },
    {
        "id": "oil-gas",
        "name": "Oil & Gas",
        "abbr": "OIL",
        "color": "#4ab8e8",
        "stocks": ["OGDC","PPL","POL","MARI","PARCO","PRL","NRL","APL","ATRL","BYCO","CNERGY"],
    },
    {
        "id": "banking",
        "name": "Banking",
        "abbr": "BNK",
        "color": "#58e8a0",
        "stocks": ["HBL","UBL","MCB","NBP","ABL","BAHL","BAFL","MEBL","AKBL","BOK","BOP","SILK","JSBL","SNBL","FAYS"],
    },
    {
        "id": "fertilizer",
        "name": "Fertilizer",
        "abbr": "FRT",
        "color": "#a0e858",
        "stocks": ["ENGRO","FFC","FFBL","FATIMA","EFERT"],
    },
    {
        "id": "power",
        "name": "Power Generation",
        "abbr": "PWR",
        "color": "#e8d840",
        "stocks": ["HUBC","KAPCO","NCPL","PKGP","SPWL","EPQL","LALPIR","JPGL","TPLP","KEL","LTPL","ATBA"],
    },
    {
        "id": "textile",
        "name": "Textile",
        "abbr": "TXT",
        "color": "#b888ff",
        "stocks": ["NCL","NML","GATM","KTML","CRTM","GTHM","TREET","KOHAT","ILP","DHTX","EPCL"],
    },
    {
        "id": "pharma",
        "name": "Pharmaceuticals",
        "abbr": "PHA",
        "color": "#e858a0",
        "stocks": ["SEARL","FEROZ","GLAXO","ABOT","HINOON","IBFL","SHEZ","PKGS"],
    },
    {
        "id": "auto",
        "name": "Automobile",
        "abbr": "AUT",
        "color": "#e84858",
        "stocks": ["PSMC","INDU","HCAR","ATLH","MTL","GHNL","SAZEW","RAVI","GHNI"],
    },
    {
        "id": "tech",
        "name": "Technology",
        "abbr": "TEC",
        "color": "#40c4e8",
        "stocks": ["SYS","TRG","NETSOL","AVN","PMRS"],
    },
    {
        "id": "steel",
        "name": "Steel & Metals",
        "abbr": "STL",
        "color": "#90a8c8",
        "stocks": ["ISL","ASTL","AGHA","MUGHAL","INIL"],
    },
    {
        "id": "food",
        "name": "Food & Beverages",
        "abbr": "FOD",
        "color": "#e8a840",
        "stocks": ["UNITY","NESTLE","QUICE","SHFA","HPUN","COLG","IBLHL"],
    },
    {
        "id": "insurance",
        "name": "Insurance",
        "abbr": "INS",
        "color": "#58b0ff",
        "stocks": ["AICL","JUBILEE","ADAMJEE","EFU","IGIL","PAKRI","SAFL","NICL"],
    },
    {
        "id": "chemicals",
        "name": "Chemicals",
        "abbr": "CHM",
        "color": "#a8e840",
        "stocks": ["EPCL","ICI","BOC","LOTPTA","NRSL"],
    },
    {
        "id": "telecom",
        "name": "Telecom",
        "abbr": "TEL",
        "color": "#e840b0",
        "stocks": ["PTC","TRG","NETSOL"],
    },
    {
        "id": "property",
        "name": "Property & REITs",
        "abbr": "PRO",
        "color": "#e8c040",
        "stocks": ["AREIT","SAIF","DOLMEN","EMCO"],
    },
]

# ── Args ──────────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="PSX Sector Monitor")
    p.add_argument("--date",  default="", help="Override date YYYY-MM-DD (default: today)")
    p.add_argument("--demo",  action="store_true", help="Generate demo data without fetching")
    return p.parse_args()

def scan_date(override=""):
    return override or date.today().isoformat()

def is_weekday(d):
    return datetime.strptime(d, "%Y-%m-%d").weekday() < 5

# ── PSX API helpers ───────────────────────────────────────────────────────────
def get_tick(symbol):
    """Fetch real-time tick data for a single symbol from psxterminal.com."""
    try:
        r = requests.get(
            f"{BASE_URL}/api/ticks/REG/{symbol}",
            headers=HEADERS, timeout=10
        )
        if r.status_code != 200:
            return None
        raw = r.json()
        tick = raw.get("data", raw) if isinstance(raw, dict) else {}
        if not tick or not isinstance(tick, dict):
            return None
        price      = float(tick.get("price",        0) or 0)
        change     = float(tick.get("change",        0) or 0)
        change_pct = float(tick.get("changePercent", 0) or 0)
        # psxterminal sometimes returns changePercent as fraction, sometimes as %
        if abs(change_pct) < 1 and price > 0 and change != 0:
            change_pct = (change / (price - change)) * 100
        volume = int(tick.get("volume", 0) or 0)
        high   = float(tick.get("high", 0) or 0)
        low    = float(tick.get("low",  0) or 0)
        ldcp   = float(tick.get("ldcp", price) or price)  # last day closing price
        if price <= 0:
            return None
        return {
            "sym":       symbol,
            "price":     round(price, 2),
            "change":    round(change, 2),
            "chg_pct":   round(change_pct, 2),
            "volume":    volume,
            "high":      round(high, 2),
            "low":       round(low, 2),
            "prev":      round(ldcp, 2),
        }
    except Exception as e:
        print(f"    [TICK ERR] {symbol}: {e}")
        return None

def fetch_all_ticks(symbols):
    """Fetch ticks for all symbols, with rate limiting."""
    results = {}
    print(f"  Fetching {len(symbols)} ticks...")
    for i, sym in enumerate(symbols):
        tick = get_tick(sym)
        if tick:
            results[sym] = tick
        if (i + 1) % 20 == 0:
            print(f"    {i+1}/{len(symbols)} done ({len(results)} valid)")
            time.sleep(0.3)
        else:
            time.sleep(0.05)
    print(f"  Done — {len(results)}/{len(symbols)} valid ticks")
    return results

# ── Sector aggregation ────────────────────────────────────────────────────────
def compute_year_start_date():
    today = date.today()
    return date(today.year, 1, 1).isoformat()

def load_history_for_periods(today_str):
    """Load existing data to compute multi-period returns."""
    history = {}  # date -> {sector_id -> chg_pct}
    index_path = os.path.join(OUTPUT_DIR, "index.json")
    if not os.path.exists(index_path):
        return history
    try:
        with open(index_path) as f:
            all_days = json.load(f)
        for day in all_days:
            d = day.get("date", "")
            if d and d != today_str:
                history[d] = {}
                for sec in day.get("sectors", []):
                    history[d][sec["id"]] = sec.get("chg_pct", 0)
    except Exception as e:
        print(f"  [HISTORY] Could not load: {e}")
    return history

def compute_period_return(sec_id, period, today_str, history):
    """
    Compute cumulative return for a given period by compounding daily changes.
    period: '5D'|'1M'|'3M'|'6M'|'YTD'|'1Y'|'5Y'
    """
    today = datetime.strptime(today_str, "%Y-%m-%d").date()
    if period == "5D":
        start = today - timedelta(days=7)
    elif period == "1M":
        start = today - timedelta(days=31)
    elif period == "3M":
        start = today - timedelta(days=92)
    elif period == "6M":
        start = today - timedelta(days=183)
    elif period == "YTD":
        start = date(today.year, 1, 1)
    elif period == "1Y":
        start = today - timedelta(days=365)
    elif period == "5Y":
        start = today - timedelta(days=365 * 5)
    else:
        return 0.0

    start_str = start.isoformat()
    # Compound daily returns
    cumulative = 1.0
    found_days = 0
    for d in sorted(history.keys()):
        if d <= start_str or d > today_str:
            continue
        sec_chg = history[d].get(sec_id, 0)
        cumulative *= (1 + sec_chg / 100)
        found_days += 1

    if found_days == 0:
        return 0.0
    return round((cumulative - 1) * 100, 2)

def build_sector_data(sector, ticks, today_str, history):
    """Aggregate tick data for a single sector."""
    total_volume = 0
    total_mcap   = 0
    weighted_chg = 0.0
    weight_sum   = 0.0
    stock_data   = []

    for sym in sector["stocks"]:
        tick = ticks.get(sym)
        if not tick:
            continue
        price  = tick["price"]
        chg    = tick["chg_pct"]
        vol    = tick["volume"]
        # Use volume-weighted average for sector change
        vol_weight = vol if vol > 0 else 1
        weighted_chg += chg * vol_weight
        weight_sum   += vol_weight
        total_volume += vol
        # Rough market cap: price * estimated shares (use volume as proxy)
        total_mcap   += price * vol
        stock_data.append({"sym": sym, "chg_pct": chg, "price": price, "volume": vol})

    if not stock_data:
        return None

    sector_chg = round(weighted_chg / weight_sum, 2) if weight_sum > 0 else 0.0
    # Sort for top/bottom performers
    sorted_stocks = sorted(stock_data, key=lambda x: x["chg_pct"], reverse=True)

    # Compute multi-period returns using historical data
    hist_returns = {"1D": sector_chg}
    for period in ["5D", "1M", "3M", "6M", "YTD", "1Y", "5Y"]:
        hist_returns[period] = compute_period_return(sector["id"], period, today_str, history)

    return {
        "id":       sector["id"],
        "name":     sector["name"],
        "abbr":     sector["abbr"],
        "color":    sector["color"],
        "index":    round(100 + sector_chg, 2),  # rebased to 100
        "chg_pct":  sector_chg,
        "volume":   total_volume,
        "mcap":     total_mcap,
        "stocks_tracked": len(stock_data),
        "stocks_total":   len(sector["stocks"]),
        "top":      sorted_stocks[:5],    # top 5 gainers
        "bottom":   sorted_stocks[-5:],   # bottom 5 losers
        "all":      sorted_stocks,        # all stocks
        "hist":     hist_returns,
    }

# ── File I/O ──────────────────────────────────────────────────────────────────
def load_index():
    path = os.path.join(OUTPUT_DIR, "index.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return []

def save_day(today_str, payload):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # Save individual day file
    day_path = os.path.join(OUTPUT_DIR, f"{today_str}.json")
    with open(day_path, "w") as f:
        json.dump(payload, f, separators=(",", ":"))
    print(f"  Saved: {day_path}")

    # Update index.json — remove old entry for this date, append new
    all_days = load_index()
    all_days = [d for d in all_days if d.get("date") != today_str]
    all_days.append(payload)
    all_days.sort(key=lambda x: x["date"])
    # Trim to MAX_HISTORY_DAYS
    if len(all_days) > MAX_HISTORY_DAYS:
        all_days = all_days[-MAX_HISTORY_DAYS:]
    index_path = os.path.join(OUTPUT_DIR, "index.json")
    with open(index_path, "w") as f:
        json.dump(all_days, f, separators=(",", ":"))
    print(f"  Updated index: {index_path} ({len(all_days)} days)")

# ── Demo data generator ───────────────────────────────────────────────────────
def generate_demo(today_str):
    """Generate 90 days of realistic demo data without any API calls."""
    print(f"  [DEMO] Generating 90 days of demo sector data...")
    import random
    rng = random.Random(42)  # seeded for reproducibility

    # Build list of weekdays for last 90 days
    end   = datetime.strptime(today_str, "%Y-%m-%d").date()
    start = end - timedelta(days=130)
    dates = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            dates.append(d.isoformat())
        d += timedelta(days=1)

    # Track cumulative index level per sector
    levels = {sec["id"]: 100.0 for sec in SECTORS}

    all_days = []
    for dt in dates:
        sectors_out = []
        for sec in SECTORS:
            daily_chg = round(rng.gauss(0.15, 2.2), 2)
            levels[sec["id"]] = round(levels[sec["id"]] * (1 + daily_chg / 100), 2)
            volume = int(rng.uniform(3e8, 3e9))
            mcap   = int(rng.uniform(5e10, 5e11))

            stock_data = []
            for sym in sec["stocks"]:
                sc = round(daily_chg + rng.gauss(0, 2.5), 2)
                pr = round(rng.uniform(20, 500), 2)
                vl = int(rng.uniform(1e5, 5e6))
                stock_data.append({"sym": sym, "chg_pct": sc, "price": pr, "volume": vl})

            stock_data.sort(key=lambda x: x["chg_pct"], reverse=True)
            sectors_out.append({
                "id":      sec["id"],
                "name":    sec["name"],
                "abbr":    sec["abbr"],
                "color":   sec["color"],
                "index":   levels[sec["id"]],
                "chg_pct": daily_chg,
                "volume":  volume,
                "mcap":    mcap,
                "stocks_tracked": len(sec["stocks"]),
                "stocks_total":   len(sec["stocks"]),
                "top":    stock_data[:5],
                "bottom": stock_data[-5:],
                "all":    stock_data,
                "hist":   {"1D": daily_chg},  # periods computed after
            })
        all_days.append({"date": dt, "sectors": sectors_out, "scanned_at": dt + "T17:30:00+05:00"})

    # Now compute multi-period hist returns properly
    history = {}
    for day in all_days:
        history[day["date"]] = {s["id"]: s["chg_pct"] for s in day["sectors"]}

    for day in all_days:
        for sec in day["sectors"]:
            for period in ["5D","1M","3M","6M","YTD","1Y","5Y"]:
                sec["hist"][period] = compute_period_return(sec["id"], period, day["date"], history)

    # Save all days
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for day in all_days:
        path = os.path.join(OUTPUT_DIR, f"{day['date']}.json")
        with open(path, "w") as f:
            json.dump(day, f, separators=(",", ":"))
    index_path = os.path.join(OUTPUT_DIR, "index.json")
    with open(index_path, "w") as f:
        json.dump(all_days, f, separators=(",", ":"))
    print(f"  [DEMO] Done — {len(all_days)} days written to {OUTPUT_DIR}/")
    print(f"  [DEMO] Date range: {all_days[0]['date']} → {all_days[-1]['date']}")

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    args = parse_args()
    today_str = scan_date(args.date)

    print(f"\n{'='*60}")
    print(f"  PSX Sector Monitor — {today_str}")
    print(f"{'='*60}")

    if args.demo:
        generate_demo(today_str)
        return

    if not is_weekday(today_str):
        print(f"  {today_str} is a weekend — skipping.")
        return

    # Load historical data for period computations
    print(f"\n  Loading historical data for period returns...")
    history = load_history_for_periods(today_str)
    print(f"  {len(history)} historical days loaded")

    # Collect all unique symbols across all sectors
    all_symbols = list({sym for sec in SECTORS for sym in sec["stocks"]})
    print(f"\n  Fetching live ticks for {len(all_symbols)} symbols...")

    ticks = fetch_all_ticks(all_symbols)

    if not ticks:
        print("  ERROR: No ticks fetched — market may be closed or API unavailable.")
        sys.exit(1)

    # Build sector aggregates
    print(f"\n  Building sector aggregates...")
    sectors_out = []
    for sec in SECTORS:
        sd = build_sector_data(sec, ticks, today_str, history)
        if sd:
            sectors_out.append(sd)
            chg = sd["chg_pct"]
            tracked = sd["stocks_tracked"]
            total   = sd["stocks_total"]
            print(f"    {sec['abbr']:<4} {sec['name']:<22} {chg:+.2f}%  ({tracked}/{total} stocks)")
        else:
            print(f"    {sec['abbr']:<4} {sec['name']:<22} NO DATA")

    if not sectors_out:
        print("  ERROR: No sector data produced.")
        sys.exit(1)

    # Sort sectors by today's change (best first)
    sectors_out.sort(key=lambda x: x["chg_pct"], reverse=True)

    payload = {
        "date":       today_str,
        "scanned_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "market":     "PSX",
        "note":       "Collected 2 hrs after PSX close (5:30 PM PKT)",
        "ticks_fetched": len(ticks),
        "sectors":    sectors_out,
    }

    save_day(today_str, payload)

    print(f"\n  ✓ Sector data saved for {today_str}")
    print(f"  ✓ {len(sectors_out)} sectors · {len(ticks)} ticks")
    gainers = sum(1 for s in sectors_out if s["chg_pct"] >= 0)
    losers  = len(sectors_out) - gainers
    print(f"  ✓ {gainers} advancing · {losers} declining")
    best  = max(sectors_out, key=lambda x: x["chg_pct"])
    worst = min(sectors_out, key=lambda x: x["chg_pct"])
    print(f"  ✓ Best:  {best['name']} {best['chg_pct']:+.2f}%")
    print(f"  ✓ Worst: {worst['name']} {worst['chg_pct']:+.2f}%")

if __name__ == "__main__":
    main()
