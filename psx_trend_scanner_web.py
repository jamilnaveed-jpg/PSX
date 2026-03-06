"""
PSX Trend Scanner — Web Version v1
=====================================================================
Replaces the email version. Saves results to:
  trend_data/YYYY-MM-DD.json  — one file per trading day
  trend_data/index.json       — sorted list of all dates

LISTS (all require avg daily volume >= 100K):
  list1 — Up 50%+   in last 20 days  (Bullish)
  list2 — Down 50%+ in last 20 days  (Bearish)
  list3 — Up 20%+   in last 5 days   (Bullish)
  list4 — Down 20%+ in last 5 days   (Bearish)
  list5 — Up PKR 20+ in last 5 days  (Bullish)
  list6 — Down PKR 20+ in last 5 days(Bearish)
=====================================================================
"""

import requests, json, time, os, sys
from datetime import datetime

BASE_URL       = "https://psxterminal.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Accept":     "application/json, */*",
    "Origin":     "https://psxterminal.com",
    "Referer":    "https://psxterminal.com/",
}
MIN_AVG_VOLUME = 100_000
PCT_50         = 50.0
PCT_20         = 20.0
PKR_20         = 20.0
OUTPUT_DIR     = "trend_data"


def get_symbols():
    resp = requests.get(f"{BASE_URL}/api/symbols", headers=HEADERS, timeout=20)
    data = resp.json()
    raw  = data.get("data", data) if isinstance(data, dict) else data
    syms = [str(s).strip() for s in raw if s]
    print(f"[SYMBOLS] {len(syms)} fetched"); return syms


def get_sector_map(symbols):
    sm = {}
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
        except Exception: sm[sym] = "—"
        if (i+1) % 100 == 0: print(f"[SECTORS] {i+1}/{len(symbols)}")
        if (i+1) % 50  == 0: time.sleep(0.2)
    return sm


def get_klines(symbol, limit=25):
    url = f"{BASE_URL}/api/klines/{symbol}/1d?limit={limit}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        if r.status_code != 200: return None
        data = r.json()
        raw  = data
        if isinstance(data, dict):
            raw = (data.get("data") or data.get("candles") or
                   data.get("klines") or data.get("result") or [])
        if not isinstance(raw, list) or len(raw) < 2: return None
        candles = []
        for c in raw:
            if not isinstance(c, dict): continue
            close  = float(c.get("close",  c.get("c", 0)) or 0)
            volume = int(  c.get("volume", c.get("v", 0)) or 0)
            ts     = c.get("timestamp", c.get("time", c.get("t", 0)))
            if close > 0: candles.append({"close": close, "volume": volume, "ts": ts})
        if len(candles) < 2: return None
        try: candles.sort(key=lambda x: x["ts"])
        except: pass
        return candles
    except: return None


def analyse_symbol(symbol, sector):
    candles = get_klines(symbol, limit=25)
    if not candles or len(candles) < 6: return None
    today_price  = candles[-1]["close"]
    today_volume = candles[-1]["volume"]
    if today_price <= 0: return None

    if len(candles) >= 6:
        p5        = candles[-6]["close"]
        last5     = candles[-5:]
        avg_v5    = sum(c["volume"] for c in last5) / len(last5)
        pct5      = round((today_price - p5) / p5 * 100, 2) if p5 > 0 else 0
        pkr5      = round(today_price - p5, 2)
    else:
        p5 = avg_v5 = pct5 = pkr5 = 0

    if len(candles) >= 21:
        p20       = candles[-21]["close"]
        last20    = candles[-20:]
        avg_v20   = sum(c["volume"] for c in last20) / len(last20)
        pct20     = round((today_price - p20) / p20 * 100, 2) if p20 > 0 else 0
        pkr20     = round(today_price - p20, 2)
    elif len(candles) >= 6:
        p20       = candles[0]["close"]
        last20    = candles
        avg_v20   = sum(c["volume"] for c in last20) / len(last20)
        pct20     = round((today_price - p20) / p20 * 100, 2) if p20 > 0 else 0
        pkr20     = round(today_price - p20, 2)
    else:
        p20 = avg_v20 = pct20 = pkr20 = 0

    return {
        "symbol":        symbol,
        "sector":        sector,
        "price":         round(today_price, 2),
        "volume":        today_volume,
        "price_5d_ago":  round(p5, 2),
        "pct_5d":        pct5,
        "pkr_5d":        pkr5,
        "avg_vol_5d":    round(avg_v5, 0),
        "price_20d_ago": round(p20, 2),
        "pct_20d":       pct20,
        "pkr_20d":       pkr20,
        "avg_vol_20d":   round(avg_v20, 0),
    }


def fetch_trend_data(symbols, sm):
    results = []; errors = 0; total = len(symbols)
    print(f"\n[TREND] Fetching {total} klines...")
    for i, sym in enumerate(symbols):
        try:
            r = analyse_symbol(sym, sm.get(sym, "—"))
            if r: results.append(r)
        except: errors += 1
        if (i+1) % 100 == 0: print(f"[TREND] {i+1}/{total} — {len(results)} valid, {errors} errors")
        if (i+1) % 50  == 0: time.sleep(0.3)
    print(f"[TREND] Done — {len(results)} stocks, {errors} errors")
    return results


def build_trend_lists(results):
    l1,l2,l3,l4,l5,l6 = [],[],[],[],[],[]
    for r in results:
        v5  = r["avg_vol_5d"]  >= MIN_AVG_VOLUME
        v20 = r["avg_vol_20d"] >= MIN_AVG_VOLUME
        if v20 and r["pct_20d"] >= PCT_50:  l1.append(r)
        if v20 and r["pct_20d"] <= -PCT_50: l2.append(r)
        if v5  and r["pct_5d"]  >= PCT_20:  l3.append(r)
        if v5  and r["pct_5d"]  <= -PCT_20: l4.append(r)
        if v5  and r["pkr_5d"]  >= PKR_20:  l5.append(r)
        if v5  and r["pkr_5d"]  <= -PKR_20: l6.append(r)
    l1.sort(key=lambda x: x["pct_20d"],  reverse=True)
    l2.sort(key=lambda x: x["pct_20d"])
    l3.sort(key=lambda x: x["pct_5d"],   reverse=True)
    l4.sort(key=lambda x: x["pct_5d"])
    l5.sort(key=lambda x: x["pkr_5d"],   reverse=True)
    l6.sort(key=lambda x: x["pkr_5d"])
    return l1,l2,l3,l4,l5,l6


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
    print(f"\n{'='*60}\n  PSX Trend Scanner (Web) — {date_str}\n{'='*60}\n")
    symbols = get_symbols()
    if not symbols: sys.exit(1)
    sm      = get_sector_map(symbols)
    results = fetch_trend_data(symbols, sm)
    if not results: sys.exit(1)
    l1,l2,l3,l4,l5,l6 = build_trend_lists(results)
    save_json(date_str, {"date":date_str,"total":len(results),
                         "list1":l1,"list2":l2,"list3":l3,
                         "list4":l4,"list5":l5,"list6":l6})
    print(f"\n  🚀{len(l1)} 💥{len(l2)} 📈{len(l3)} 📉{len(l4)} 💰{len(l5)} 🔻{len(l6)}\n")

if __name__ == "__main__":
    main()
