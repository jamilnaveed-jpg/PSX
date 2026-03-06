"""
PSX Daily Scanner — Web Version v1
=====================================================================
Replaces the email version. Saves results to:
  daily_data/YYYY-MM-DD.json  — one file per trading day
  daily_data/index.json       — sorted list of all dates

LISTS:
  list1 — Momentum Gainers  : change >= +4%,  volume >= 100K
  list2 — High-Volume Movers: volume >= 9M    (any direction)
  list3 — Tight Range Watch : |change| <= 0.40%
  list4 — Top Losers        : change <= -4%,  volume >= 100K
  market — breadth + per-sector breakdown
=====================================================================
"""

import requests, json, time, os, sys
from datetime import datetime

BASE_URL  = "https://psxterminal.com"
HEADERS   = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Accept":     "application/json, */*",
    "Origin":     "https://psxterminal.com",
    "Referer":    "https://psxterminal.com/",
}
GAINER_PCT      = 4.0
LOSER_PCT       = -4.0
MIN_VOLUME_L1   = 100_000
MIN_VOLUME_L2   = 9_000_000
TIGHT_RANGE_PCT = 0.40
OUTPUT_DIR      = "daily_data"


def get_symbols():
    resp = requests.get(f"{BASE_URL}/api/symbols", headers=HEADERS, timeout=20)
    data = resp.json()
    raw  = data.get("data", data) if isinstance(data, dict) else data
    syms = [str(s).strip() for s in raw if s]
    print(f"[SYMBOLS] {len(syms)} fetched"); return syms


def get_sector_map(symbols):
    sm = {}; errors = 0
    print(f"[SECTORS] Fetching {len(symbols)} sectors...")
    for i, sym in enumerate(symbols):
        try:
            r = requests.get(f"{BASE_URL}/api/companies/{sym}", headers=HEADERS, timeout=8)
            if r.status_code == 200:
                info = r.json().get("data", {}) if isinstance(r.json(), dict) else {}
                sec  = (info.get("sector") or info.get("sectorName") or
                        info.get("industry") or "—") if isinstance(info, dict) else "—"
                sm[sym] = str(sec).strip() or "—"
            else: sm[sym] = "—"
        except Exception: sm[sym] = "—"; errors += 1
        if (i+1) % 100 == 0: print(f"[SECTORS] {i+1}/{len(symbols)}, errors:{errors}")
        if (i+1) % 50  == 0: time.sleep(0.2)
    print(f"[SECTORS] Done — {sum(1 for v in sm.values() if v!='—')}/{len(symbols)} found")
    return sm


def get_tick(symbol):
    r = requests.get(f"{BASE_URL}/api/ticks/REG/{symbol}", headers=HEADERS, timeout=10)
    if r.status_code != 200: return None
    tick = r.json().get("data", {}) if isinstance(r.json(), dict) else {}
    if not tick or not isinstance(tick, dict): return None
    price      = float(tick.get("price",        0) or 0)
    change     = float(tick.get("change",        0) or 0)
    change_pct = float(tick.get("changePercent", 0) or 0) * 100
    volume     = int(  tick.get("volume",        0) or 0)
    high       = float(tick.get("high",          0) or 0)
    low        = float(tick.get("low",           0) or 0)
    if price <= 0: return None
    return {"symbol": symbol, "sector": "—",
            "price": round(price,2), "change": round(change,2),
            "change_pct": round(change_pct,2), "volume": volume,
            "high": round(high,2), "low": round(low,2)}


def fetch_all_stocks(symbols, sm):
    stocks = []; errors = 0; total = len(symbols)
    print(f"\n[TICKS] Fetching {total} ticks...")
    for i, sym in enumerate(symbols):
        try:
            t = get_tick(sym)
            if t: t["sector"] = sm.get(sym, "—"); stocks.append(t)
        except Exception: errors += 1
        if (i+1) % 100 == 0: print(f"[TICKS] {i+1}/{total} — {len(stocks)} valid")
        if (i+1) % 50  == 0: time.sleep(0.3)
    print(f"[TICKS] Done — {len(stocks)} stocks"); return stocks


def build_market_summary(stocks):
    gainers   = sum(1 for s in stocks if s["change_pct"] > 0)
    losers    = sum(1 for s in stocks if s["change_pct"] < 0)
    unchanged = len(stocks) - gainers - losers
    total_vol = sum(s["volume"] for s in stocks)
    total_val = sum(s["price"] * s["volume"] for s in stocks)
    sectors = {}
    for s in stocks:
        sec = s["sector"] if s["sector"] not in ("—","",None) else "Unknown"
        if sec not in sectors:
            sectors[sec] = {"stocks":[],"total_vol":0,"total_val":0.0,
                            "gainers":0,"losers":0,"unchanged":0,"wtd_pct":0.0}
        d = sectors[sec]; d["stocks"].append(s)
        d["total_vol"] += s["volume"]; d["total_val"] += s["price"]*s["volume"]
        d["wtd_pct"]   += s["change_pct"]*s["volume"]
        if   s["change_pct"] > 0: d["gainers"]   += 1
        elif s["change_pct"] < 0: d["losers"]    += 1
        else:                     d["unchanged"] += 1
    sector_list = []
    for name, d in sectors.items():
        avg_pct = round(d["wtd_pct"]/d["total_vol"], 2) if d["total_vol"] > 0 else 0.0
        top3 = sorted(d["stocks"], key=lambda x: x["volume"], reverse=True)[:3]
        sector_list.append({"name": name, "total_vol": d["total_vol"],
            "total_val": round(d["total_val"],0), "gainers": d["gainers"],
            "losers": d["losers"], "unchanged": d["unchanged"],
            "count": len(d["stocks"]), "avg_pct": avg_pct,
            "top3": [{"symbol":t["symbol"],"price":t["price"],
                      "change_pct":t["change_pct"],"volume":t["volume"]} for t in top3]})
    sector_list.sort(key=lambda x: x["total_vol"], reverse=True)
    return {"gainers":gainers,"losers":losers,"unchanged":unchanged,"total":len(stocks),
            "total_vol":total_vol,"total_val":round(total_val,0),"sectors":sector_list}


def build_lists(stocks):
    l1,l2,l3,l4 = [],[],[],[]
    for s in stocks:
        p,v = s["change_pct"],s["volume"]
        if p >= GAINER_PCT and v >= MIN_VOLUME_L1: l1.append(s)
        if v >= MIN_VOLUME_L2:                     l2.append(s)
        if abs(p) <= TIGHT_RANGE_PCT:              l3.append(s)
        if p <= LOSER_PCT  and v >= MIN_VOLUME_L1: l4.append(s)
    l1.sort(key=lambda x: x["change_pct"], reverse=True)
    l2.sort(key=lambda x: x["volume"],     reverse=True)
    l3.sort(key=lambda x: abs(x["change_pct"]))
    l4.sort(key=lambda x: x["change_pct"])
    return l1,l2,l3,l4


def save_json(date_str, data):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f"{date_str}.json")
    with open(path,"w") as f: json.dump(data,f,separators=(",",":"))
    print(f"[SAVE] {path} ({os.path.getsize(path)//1024} KB)")
    idx_path = os.path.join(OUTPUT_DIR, "index.json")
    dates = json.load(open(idx_path)) if os.path.exists(idx_path) else []
    if date_str not in dates: dates.append(date_str)
    dates.sort(reverse=True)
    with open(idx_path,"w") as f: json.dump(dates,f)
    print(f"[INDEX] {len(dates)} dates on record")


def main():
    date_str = datetime.now().strftime("%Y-%m-%d")
    print(f"\n{'='*60}\n  PSX Daily Scanner (Web) — {date_str}\n{'='*60}\n")
    symbols = get_symbols()
    if not symbols: sys.exit(1)
    sm      = get_sector_map(symbols)
    stocks  = fetch_all_stocks(symbols, sm)
    if not stocks: sys.exit(1)
    market       = build_market_summary(stocks)
    l1,l2,l3,l4 = build_lists(stocks)
    save_json(date_str, {"date":date_str,"market":market,
                         "list1":l1,"list2":l2,"list3":l3,"list4":l4})
    print(f"\n  ▲{market['gainers']} ▼{market['losers']} —{market['unchanged']} | "
          f"L1:{len(l1)} L2:{len(l2)} L3:{len(l3)} L4:{len(l4)}\n")

if __name__ == "__main__":
    main()
