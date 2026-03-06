"""
PSX Market Monitor — Data Updater v2
=====================================================================
CHANGES FROM v1:
  - t2108  : % stocks above 40-day MA  (T2108 column)
  - t2107  : % stocks above 200-day MA (T2107 column)
  - kse100 : KSE-100 index closing value
  - Klines limit raised to 210 to cover 200-day MA calculation
  - Schedule moved to 5:00 PM PKT (12:00 UTC)

COLUMNS:
Primary Breadth:
  up4     — stocks up 4%+ today
  dn4     — stocks down 4%+ today
  r5      — 5-day ratio
  r10     — 10-day ratio

Secondary Breadth:
  up25q, dn25q  — up/down 25%+ in quarter (65d)
  up25m, dn25m  — up/down 25%+ in month (21d)
  up50m, dn50m  — up/down 50%+ in month
  up13_34, dn13_34 — up/down 13%+ in 34 days
  total         — total stocks scanned

New columns:
  t2108   — % of stocks trading above their 40-day MA
  t2107   — % of stocks trading above their 200-day MA
  kse100  — KSE-100 index value
=====================================================================
"""

import requests
import json
import time
import os
import sys
from datetime import datetime

BASE_URL = "https://psxterminal.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Accept":     "application/json, */*",
    "Origin":     "https://psxterminal.com",
    "Referer":    "https://psxterminal.com/",
}

MIN_AVG_VOL  = 100_000
DATA_FILE    = "mm_data.json"
MAX_ROWS     = 365          # keep rolling 1 year of data


# ── Fetch all symbols ──────────────────────────────────────────────────────────
def get_symbols():
    resp = requests.get(f"{BASE_URL}/api/symbols", headers=HEADERS, timeout=20)
    data = resp.json()
    raw  = data.get("data", data) if isinstance(data, dict) else data
    syms = [str(s).strip() for s in raw if s]
    print(f"[SYMBOLS] {len(syms)} fetched")
    return syms


# ── Fetch KSE-100 index value ──────────────────────────────────────────────────
def get_kse100():
    """
    Fetch latest KSE-100 index value from psxterminal.
    Tries /api/ticks/IDX/KSE100 first, then /api/stats/IDX as fallback.
    """
    endpoints = [
        f"{BASE_URL}/api/ticks/IDX/KSE100",
        f"{BASE_URL}/api/ticks/IDX/KSE-100",
        f"{BASE_URL}/api/indices/KSE100",
        f"{BASE_URL}/api/stats/IDX",
    ]
    for url in endpoints:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            if resp.status_code != 200:
                continue
            data = resp.json()
            # Try common field paths
            for path in [
                lambda d: d.get("data", {}).get("price"),
                lambda d: d.get("data", {}).get("close"),
                lambda d: d.get("data", {}).get("last"),
                lambda d: d.get("price"),
                lambda d: d.get("close"),
            ]:
                try:
                    val = path(data)
                    if val and float(val) > 0:
                        v = round(float(val), 2)
                        print(f"[KSE100] {v:,.2f}  (from {url})")
                        return v
                except Exception:
                    pass
        except Exception:
            pass
    print("[KSE100] ⚠ Could not fetch — will store None")
    return None


# ── Fetch klines for one symbol ────────────────────────────────────────────────
def get_klines(symbol, limit=210):
    """
    Returns candles sorted oldest→newest, or None on failure.
    limit=70 covers 65 trading days (quarter) with buffer.
    """
    url = f"{BASE_URL}/api/klines/{symbol}/1d?limit={limit}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=12)
        if resp.status_code != 200:
            return None
        data = resp.json()
        raw  = data
        if isinstance(data, dict):
            raw = (data.get("data") or data.get("candles") or
                   data.get("klines") or data.get("result") or [])
        if not isinstance(raw, list) or len(raw) < 2:
            return None

        candles = []
        for c in raw:
            if not isinstance(c, dict):
                continue
            close  = float(c.get("close",  c.get("c", 0)) or 0)
            volume = int(  c.get("volume", c.get("v", 0)) or 0)
            ts     = c.get("timestamp", c.get("time", c.get("t", 0)))
            if close > 0:
                candles.append({"close": close, "volume": volume, "ts": ts})

        if len(candles) < 2:
            return None
        try:
            candles.sort(key=lambda x: x["ts"])
        except Exception:
            pass
        return candles
    except Exception:
        return None


# ── Compute metrics for one symbol ─────────────────────────────────────────────
def analyse(symbol):
    candles = get_klines(symbol, limit=210)
    if not candles or len(candles) < 6:
        return None

    today = candles[-1]["close"]
    if today <= 0:
        return None

    def lookback(n):
        if len(candles) < n + 1:
            return None, 0
        base    = candles[-(n + 1)]["close"]
        window  = candles[-n:]
        avg_vol = sum(c["volume"] for c in window) / len(window)
        if base <= 0:
            return None, avg_vol
        return round((today - base) / base * 100, 2), avg_vol

    pct1,  vol1  = lookback(1)
    pct21, vol21 = lookback(21)
    pct34, vol34 = lookback(34)
    pct65, vol65 = lookback(65)

    # ── 40-day MA (T2108) ──────────────────────────────────────────────────
    above_40ma = None
    if len(candles) >= 40:
        ma40 = sum(c["close"] for c in candles[-40:]) / 40
        above_40ma = today > ma40

    # ── 200-day MA (T2107) ─────────────────────────────────────────────────
    above_200ma = None
    if len(candles) >= 200:
        ma200 = sum(c["close"] for c in candles[-200:]) / 200
        above_200ma = today > ma200

    return {
        "symbol":     symbol,
        "price":      today,
        "pct1":       pct1,   "vol1":  vol1,
        "pct21":      pct21,  "vol21": vol21,
        "pct34":      pct34,  "vol34": vol34,
        "pct65":      pct65,  "vol65": vol65,
        "above_40ma":  above_40ma,
        "above_200ma": above_200ma,
    }


# ── Fetch all stock data ───────────────────────────────────────────────────────
def fetch_all(symbols):
    results = []
    errors  = 0
    total   = len(symbols)
    print(f"[DATA] Fetching klines for {total} symbols...")

    for i, sym in enumerate(symbols):
        try:
            r = analyse(sym)
            if r:
                results.append(r)
        except Exception:
            errors += 1

        if (i + 1) % 100 == 0:
            print(f"[DATA] {i+1}/{total} — {len(results)} valid, {errors} errors")
        if (i + 1) % 50 == 0:
            time.sleep(0.3)

    print(f"[DATA] Done — {len(results)} stocks, {errors} errors")
    return results


# ── Compute today's MM row ─────────────────────────────────────────────────────
def compute_row(results, date_str, kse100_val):
    up4 = dn4 = 0
    up25q = dn25q = up25m = dn25m = 0
    up50m = dn50m = up13_34 = dn13_34 = 0
    total = len(results)

    above40_count  = 0
    above40_total  = 0
    above200_count = 0
    above200_total = 0

    for r in results:
        # Today 4% moves
        if r["pct1"] is not None and r["vol1"] >= MIN_AVG_VOL:
            if r["pct1"] >= 4.0:   up4 += 1
            elif r["pct1"] <= -4.0: dn4 += 1

        # Quarter 25%
        if r["pct65"] is not None and r["vol65"] >= MIN_AVG_VOL:
            if r["pct65"] >= 25.0:   up25q += 1
            elif r["pct65"] <= -25.0: dn25q += 1

        # Month 25%
        if r["pct21"] is not None and r["vol21"] >= MIN_AVG_VOL:
            if r["pct21"] >= 25.0:   up25m += 1
            elif r["pct21"] <= -25.0: dn25m += 1

        # Month 50%
        if r["pct21"] is not None and r["vol21"] >= MIN_AVG_VOL:
            if r["pct21"] >= 50.0:   up50m += 1
            elif r["pct21"] <= -50.0: dn50m += 1

        # 34-day 13%
        if r["pct34"] is not None and r["vol34"] >= MIN_AVG_VOL:
            if r["pct34"] >= 13.0:   up13_34 += 1
            elif r["pct34"] <= -13.0: dn13_34 += 1

        # 40-day MA (T2108)
        if r["above_40ma"] is not None:
            above40_total += 1
            if r["above_40ma"]:
                above40_count += 1

        # 200-day MA (T2107)
        if r["above_200ma"] is not None:
            above200_total += 1
            if r["above_200ma"]:
                above200_count += 1

    t2108 = round(above40_count  / above40_total  * 100, 1) if above40_total  > 0 else None
    t2107 = round(above200_count / above200_total * 100, 1) if above200_total > 0 else None

    return {
        "date":    date_str,
        "up4":     up4,
        "dn4":     dn4,
        "r5":      None,
        "r10":     None,
        "up25q":   up25q,
        "dn25q":   dn25q,
        "up25m":   up25m,
        "dn25m":   dn25m,
        "up50m":   up50m,
        "dn50m":   dn50m,
        "up13_34": up13_34,
        "dn13_34": dn13_34,
        "total":   total,
        "t2108":   t2108,
        "t2107":   t2107,
        "kse100":  kse100_val,
    }


# ── Load / save JSON data file ─────────────────────────────────────────────────
def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE) as f:
            return json.load(f)
    return []


def compute_ratios(rows):
    """
    5-day ratio  = sum(up4 last 5 rows) / sum(dn4 last 5 rows)
    10-day ratio = sum(up4 last 10 rows) / sum(dn4 last 10 rows)
    Applied to the last row only (current day).
    """
    for i, row in enumerate(rows):
        for window, key in [(5, "r5"), (10, "r10")]:
            chunk = rows[max(0, i - window + 1): i + 1]
            su    = sum(r["up4"] for r in chunk)
            sd    = sum(r["dn4"] for r in chunk)
            row[key] = round(su / sd, 2) if sd > 0 else None
    return rows


def save_data(rows):
    # Keep rolling MAX_ROWS
    rows = rows[-MAX_ROWS:]
    rows = compute_ratios(rows)
    with open(DATA_FILE, "w") as f:
        json.dump(rows, f, indent=2)
    print(f"[SAVE] {DATA_FILE} updated — {len(rows)} rows total")


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    date_str = datetime.now().strftime("%Y-%m-%d")
    print(f"\n{'='*60}")
    print(f"  PSX Market Monitor Updater v2 — {date_str}")
    print(f"{'='*60}\n")

    symbols = get_symbols()
    if not symbols:
        print("[MAIN] ❌ No symbols. Aborting."); sys.exit(1)

    kse100_val = get_kse100()
    results    = fetch_all(symbols)
    if not results:
        print("[MAIN] ❌ No data. Aborting."); sys.exit(1)

    row  = compute_row(results, date_str, kse100_val)
    rows = load_data()

    # Replace today's row if already exists (re-run safe)
    rows = [r for r in rows if r["date"] != date_str]
    rows.append(row)
    save_data(rows)

    print(f"\n{'─'*50}")
    print(f"  Date       : {date_str}")
    print(f"  Up 4%+     : {row['up4']}")
    print(f"  Down 4%+   : {row['dn4']}")
    print(f"  Up 25% Q   : {row['up25q']}")
    print(f"  Dn 25% Q   : {row['dn25q']}")
    print(f"  Up 25% M   : {row['up25m']}")
    print(f"  Dn 25% M   : {row['dn25m']}")
    print(f"  Up 50% M   : {row['up50m']}")
    print(f"  Dn 50% M   : {row['dn50m']}")
    print(f"  Up 13% 34d : {row['up13_34']}")
    print(f"  Dn 13% 34d : {row['dn13_34']}")
    print(f"  T2108 40MA : {row['t2108']}%")
    print(f"  T2107 200MA: {row['t2107']}%")
    print(f"  KSE-100    : {row['kse100']}")
    print(f"  Total      : {row['total']}")
    print(f"{'─'*50}\n")


if __name__ == "__main__":
    main()
