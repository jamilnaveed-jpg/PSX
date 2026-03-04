"""
PSX Daily Stock Scanner — v6 (FINAL)
=====================================================================
Uses confirmed working endpoints from psxterminal.com:
  Step 1: GET /api/symbols           → fetch all 553 PSX symbols
  Step 2: GET /api/stats/REG         → fetch market stats (prices/change/volume)
  Step 3: GET /api/ticks/REG/{SYM}   → fallback per-symbol if stats incomplete

LIST 1 — Momentum Gainers   : up 4%+  AND volume >= 100,000
LIST 2 — High-Volume Movers : up 4%+  AND volume >= 9,000,000
LIST 3 — Tight Range Watch  : |change%| <= 0.40% (up OR down)
=====================================================================
"""

import requests
import smtplib
import traceback
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
import os
import sys
import json

# ── Configuration ──────────────────────────────────────────────────────────────
RECIPIENT_EMAIL  = "jamilnaveed@hotmail.com"
SENDER_EMAIL     = os.environ.get("SENDER_EMAIL")
SENDER_PASSWORD  = os.environ.get("SENDER_PASSWORD")

BASE_URL         = "https://psxterminal.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Accept":     "application/json, */*",
    "Origin":     "https://psxterminal.com",
    "Referer":    "https://psxterminal.com/",
}

GAINER_PCT       = 4.0
MIN_VOLUME_L1    = 100_000
MIN_VOLUME_L2    = 9_000_000
TIGHT_RANGE_PCT  = 0.40


# ── Step 1: Fetch all symbols ──────────────────────────────────────────────────
def fetch_symbols():
    """Returns list of symbol strings e.g. ['OGDC', 'PPL', 'LUCK', ...]"""
    url = f"{BASE_URL}/api/symbols"
    print(f"[SYMBOLS] Fetching: {url}")
    resp = requests.get(url, headers=HEADERS, timeout=20)
    print(f"[SYMBOLS] Status: {resp.status_code}, Size: {len(resp.content)} bytes")
    data = resp.json()

    raw = data.get("data", data) if isinstance(data, dict) else data
    symbols = []
    for item in raw:
        if isinstance(item, str):
            symbols.append(item.strip())
        elif isinstance(item, dict):
            sym = item.get("symbol", item.get("sym", item.get("name", "")))
            if sym:
                symbols.append(str(sym).strip())

    print(f"[SYMBOLS] Found {len(symbols)} symbols")
    if symbols:
        print(f"[SYMBOLS] Sample: {symbols[:5]}")
    return symbols


# ── Step 2: Fetch full market stats ───────────────────────────────────────────
def fetch_stats():
    """
    Tries /api/stats/REG which returns summary stats for all stocks.
    Returns list of stock dicts if successful.
    """
    url = f"{BASE_URL}/api/stats/REG"
    print(f"\n[STATS] Fetching: {url}")
    resp = requests.get(url, headers=HEADERS, timeout=20)
    print(f"[STATS] Status: {resp.status_code}, Size: {len(resp.content)} bytes")
    data = resp.json()

    raw = data.get("data", data) if isinstance(data, dict) else data
    if not isinstance(raw, list):
        # Sometimes data is nested differently - log the structure
        print(f"[STATS] Top-level keys: {list(data.keys()) if isinstance(data, dict) else type(raw)}")
        # Try to find a list anywhere in the response
        if isinstance(data, dict):
            for k, v in data.items():
                if isinstance(v, list) and len(v) > 0:
                    print(f"[STATS] Found list under key '{k}' with {len(v)} items")
                    raw = v
                    break

    print(f"[STATS] Raw records: {len(raw) if isinstance(raw, list) else 'not a list'}")

    if not isinstance(raw, list) or len(raw) == 0:
        return []

    # Log sample record to understand field names
    if isinstance(raw[0], dict):
        print(f"[STATS] Sample keys: {list(raw[0].keys())}")
        print(f"[STATS] Sample data: {json.dumps(raw[0], default=str)[:400]}")

    stocks = []
    for item in raw:
        try:
            s = parse_stock_item(item)
            if s:
                stocks.append(s)
        except Exception:
            continue

    print(f"[STATS] Parsed {len(stocks)} stocks")
    return stocks


# ── Step 3: Fetch individual ticks (fallback/supplement) ──────────────────────
def fetch_ticks_for_symbols(symbols, max_symbols=553):
    """
    Fetches /api/ticks/REG/{SYMBOL} for each symbol.
    Used when /api/stats doesn't return full data.
    Batches with small delay to avoid rate limiting.
    """
    print(f"\n[TICKS] Fetching ticks for {min(len(symbols), max_symbols)} symbols...")
    stocks = []
    errors = 0

    for i, sym in enumerate(symbols[:max_symbols]):
        url = f"{BASE_URL}/api/ticks/REG/{sym}"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                raw  = data.get("data", data) if isinstance(data, dict) else data

                # ticks may return a single dict or a list
                if isinstance(raw, dict):
                    raw = [raw]
                elif isinstance(raw, list) and len(raw) > 0:
                    pass
                else:
                    continue

                for item in raw:
                    s = parse_stock_item(item, symbol_fallback=sym)
                    if s:
                        stocks.append(s)
                        break  # only need latest tick per symbol
            else:
                errors += 1
        except Exception:
            errors += 1

        # Small delay every 50 requests to be respectful
        if (i + 1) % 50 == 0:
            print(f"[TICKS] Progress: {i+1}/{min(len(symbols), max_symbols)} symbols, {len(stocks)} parsed, {errors} errors")
            time.sleep(0.5)

    print(f"[TICKS] Complete: {len(stocks)} stocks fetched, {errors} errors")
    return stocks


def parse_stock_item(item, symbol_fallback="N/A"):
    """Parse a stock dict from any psxterminal endpoint format."""
    if not isinstance(item, dict):
        return None

    symbol = str(item.get("symbol", item.get("sym", item.get("ticker", symbol_fallback)))).strip()
    if not symbol or symbol == "N/A":
        return None

    name       = str(item.get("name", item.get("company", item.get("companyName", symbol)))).strip()
    price      = float(item.get("last",      item.get("current", item.get("close",   item.get("ltp",  0)))) or 0)
    prev_close = float(item.get("prevClose", item.get("prev",    item.get("ldcp",    item.get("pclose", 0)))) or 0)
    change     = float(item.get("change",    item.get("priceChange", 0)) or 0)
    change_pct = float(item.get("changePct", item.get("change_pct",  item.get("changep",
                        item.get("percentChange", item.get("pctChange", 0))))) or 0)
    volume     = int(  item.get("volume",    item.get("vol",     item.get("totalVolume", 0))) or 0)

    # Compute change_pct if still 0 but we have enough data
    if change_pct == 0 and prev_close > 0 and price > 0:
        change_pct = round(((price - prev_close) / prev_close) * 100, 2)
    if change_pct == 0 and prev_close > 0 and change != 0:
        change_pct = round((change / prev_close) * 100, 2)

    return {
        "symbol":     symbol,
        "name":       name,
        "price":      price,
        "prev_close": prev_close,
        "change":     change,
        "change_pct": change_pct,
        "volume":     volume,
    }


# ── Master fetch ───────────────────────────────────────────────────────────────
def fetch_all_stocks():
    """
    Strategy:
    1. Try /api/stats/REG first (one call, gets all stocks)
    2. If it returns < 10 stocks, fall back to fetching ticks per symbol
    """
    # Try stats endpoint first (fast, single call)
    try:
        stocks = fetch_stats()
        if len(stocks) >= 10:
            print(f"\n✅ Got {len(stocks)} stocks from /api/stats/REG")
            return stocks, "psxterminal.com/api/stats/REG"
        else:
            print(f"[MAIN] Stats returned only {len(stocks)} stocks, switching to per-symbol ticks...")
    except Exception as e:
        print(f"[MAIN] Stats endpoint failed: {e}")

    # Fall back to fetching all symbols then their ticks
    try:
        symbols = fetch_symbols()
        if not symbols:
            print("[MAIN] Could not fetch symbol list")
            return [], "failed"

        stocks = fetch_ticks_for_symbols(symbols)
        if stocks:
            print(f"\n✅ Got {len(stocks)} stocks from per-symbol ticks")
            return stocks, "psxterminal.com/api/ticks/REG/{symbol}"
    except Exception as e:
        print(f"[MAIN] Ticks fallback failed: {e}")
        traceback.print_exc()

    return [], "all sources failed"


# ── Filters ────────────────────────────────────────────────────────────────────
def build_lists(stocks):
    list1, list2, list3 = [], [], []
    for s in stocks:
        pct = s["change_pct"]
        vol = s["volume"]
        if pct >= GAINER_PCT and vol >= MIN_VOLUME_L1:
            list1.append(s)
        if pct >= GAINER_PCT and vol >= MIN_VOLUME_L2:
            list2.append(s)
        if abs(pct) <= TIGHT_RANGE_PCT:
            list3.append(s)
    list1.sort(key=lambda x: x["change_pct"], reverse=True)
    list2.sort(key=lambda x: x["volume"],     reverse=True)
    list3.sort(key=lambda x: abs(x["change_pct"]))
    return list1, list2, list3


# ── HTML Helpers ───────────────────────────────────────────────────────────────
def fmt_vol(v):
    if not isinstance(v, (int, float)):
        return str(v)
    if v >= 1_000_000:
        return f"{v/1_000_000:.2f}M"
    if v >= 1_000:
        return f"{v/1_000:.1f}K"
    return f"{v:,}"


def pct_badge(pct):
    if pct > 0:
        bg, fg, sign = "#e8f5e9", "#2e7d32", "+"
    elif pct < 0:
        bg, fg, sign = "#fdecea", "#c62828", ""
    else:
        bg, fg, sign = "#f5f5f5", "#555", ""
    return (
        f'<span style="background:{bg};color:{fg};padding:3px 9px;'
        f'border-radius:10px;font-weight:bold;font-size:12.5px;">'
        f'{sign}{pct:.2f}%</span>'
    )


def make_table(rows_html, col_headers):
    ths = ""
    for i, h in enumerate(col_headers):
        align = "left" if i <= 1 else "right"
        ths += f'<th style="padding:11px 13px;text-align:{align};font-weight:600;">{h}</th>'
    return f"""
    <table style="width:100%;border-collapse:collapse;font-size:13.5px;
                  font-family:Arial,sans-serif;border:1px solid #ddd;
                  border-radius:6px;overflow:hidden;margin-bottom:6px;">
      <thead><tr style="background:#1a1a2e;color:#fff;">{ths}</tr></thead>
      <tbody>{rows_html}</tbody>
    </table>"""


def sec_hdr(emoji, title, subtitle, colour):
    return f"""
    <div style="background:{colour};border-radius:8px 8px 0 0;
                padding:13px 18px;margin-top:32px;">
      <h3 style="margin:0;color:#fff;font-size:15.5px;">{emoji}&nbsp; {title}</h3>
      <p style="margin:4px 0 0;color:rgba(255,255,255,0.72);font-size:12px;">{subtitle}</p>
    </div>"""


def empty_msg(text):
    return f'<p style="color:#999;font-style:italic;padding:12px 0 24px;">{text}</p>'


# ── Email Builder ──────────────────────────────────────────────────────────────
def build_email(list1, list2, list3, scan_date, source_name, total_stocks):

    subject = (
        f"PSX Scanner {scan_date} — "
        f"Gainers:{len(list1)} | HiVol:{len(list2)} | TightRange:{len(list3)}"
    )

    # Section 1 — Momentum Gainers
    if list1:
        rows = ""
        for i, s in enumerate(list1):
            bg = "#f9fafb" if i % 2 == 0 else "#fff"
            rows += f"""<tr style="background:{bg};">
              <td style="padding:9px 13px;font-weight:bold;color:#1a1a2e;">{s['symbol']}</td>
              <td style="padding:9px 13px;color:#444;">{s['name']}</td>
              <td style="padding:9px 13px;text-align:right;">{s['price']:,.2f}</td>
              <td style="padding:9px 13px;text-align:right;">{pct_badge(s['change_pct'])}</td>
              <td style="padding:9px 13px;text-align:right;color:#555;">{fmt_vol(s['volume'])}</td>
            </tr>"""
        sec1 = (
            sec_hdr("📈","Momentum Gainers",
                f"Up 4%+ with Volume >= 100,000 &nbsp;|&nbsp; {len(list1)} stock(s) found today","#2e7d32")
            + make_table(rows, ["Symbol","Company","Price (PKR)","Change %","Volume"])
        )
    else:
        sec1 = (
            sec_hdr("📈","Momentum Gainers","Up 4%+ with Volume >= 100K","#2e7d32")
            + empty_msg("No stocks met the Momentum Gainer criteria today.")
        )

    # Section 2 — High-Volume Movers
    if list2:
        rows = ""
        for i, s in enumerate(list2):
            bg = "#fdf4f4" if i % 2 == 0 else "#fff"
            rows += f"""<tr style="background:{bg};">
              <td style="padding:9px 13px;font-weight:bold;color:#1a1a2e;">{s['symbol']}</td>
              <td style="padding:9px 13px;color:#444;">{s['name']}</td>
              <td style="padding:9px 13px;text-align:right;">{s['price']:,.2f}</td>
              <td style="padding:9px 13px;text-align:right;">{pct_badge(s['change_pct'])}</td>
              <td style="padding:9px 13px;text-align:right;font-weight:bold;color:#b71c1c;">
                  {fmt_vol(s['volume'])}
              </td>
            </tr>"""
        sec2 = (
            sec_hdr("🔥","High-Volume Power Movers",
                f"Up 4%+ with Volume >= 9,000,000 &nbsp;|&nbsp; {len(list2)} stock(s) found today","#b71c1c")
            + make_table(rows, ["Symbol","Company","Price (PKR)","Change %","Volume"])
        )
    else:
        sec2 = (
            sec_hdr("🔥","High-Volume Power Movers","Up 4%+ with Volume >= 9M","#b71c1c")
            + empty_msg("No stocks traded above 9M volume with a 4%+ gain today.")
        )

    # Section 3 — Tight Range Watch
    if list3:
        rows = ""
        for i, s in enumerate(list3):
            bg = "#f7f4ff" if i % 2 == 0 else "#fff"
            if s['change_pct'] > 0:
                direction = '<span style="color:#2e7d32;">▲ Up</span>'
            elif s['change_pct'] < 0:
                direction = '<span style="color:#c62828;">▼ Down</span>'
            else:
                direction = '<span style="color:#888;">— Flat</span>'
            rows += f"""<tr style="background:{bg};">
              <td style="padding:9px 13px;font-weight:bold;color:#1a1a2e;">{s['symbol']}</td>
              <td style="padding:9px 13px;color:#444;">{s['name']}</td>
              <td style="padding:9px 13px;text-align:right;">{s['price']:,.2f}</td>
              <td style="padding:9px 13px;text-align:right;">{pct_badge(s['change_pct'])}</td>
              <td style="padding:9px 13px;text-align:center;">{direction}</td>
              <td style="padding:9px 13px;text-align:right;color:#666;">{fmt_vol(s['volume'])}</td>
            </tr>"""
        sec3 = (
            sec_hdr("🔔","Tight Range Watch",
                f"|Change| <= 0.40% (up or down) — Consolidating candidates "
                f"&nbsp;|&nbsp; {len(list3)} stock(s) found today","#5c35a0")
            + make_table(rows, ["Symbol","Company","Price (PKR)","Change %","Direction","Volume"])
        )
    else:
        sec3 = (
            sec_hdr("🔔","Tight Range Watch","|Change| <= 0.40% (up or down)","#5c35a0")
            + empty_msg("No tight-range consolidating stocks found today.")
        )

    ok = total_stocks > 0
    status_note = f"""
    <div style="background:{'#e8f5e9' if ok else '#fff8e1'};border-radius:6px;
                padding:10px 14px;margin-top:28px;font-size:12px;color:#555;">
      {'✅' if ok else '⚠️'} <strong>Data source:</strong> {source_name}
      &nbsp;|&nbsp; <strong>Total stocks scanned:</strong> {total_stocks}
    </div>"""

    html = f"""<html>
<body style="font-family:Arial,sans-serif;max-width:860px;margin:auto;
             padding:20px;background:#f0f0f0;">
  <div style="background:#1a1a2e;padding:22px 26px;border-radius:10px 10px 0 0;">
    <h1 style="color:#fff;margin:0;font-size:22px;">📊 PSX Daily Stock Scanner</h1>
    <p style="color:#aaa;margin:6px 0 0;font-size:13px;">
      {scan_date} &nbsp;|&nbsp; End-of-Day Report &nbsp;|&nbsp; Pakistan Stock Exchange
    </p>
  </div>
  <div style="background:#fff;border:1px solid #ddd;border-top:none;
              padding:18px 26px;display:flex;gap:40px;flex-wrap:wrap;">
    <div style="text-align:center;min-width:120px;">
      <div style="font-size:30px;font-weight:bold;color:#2e7d32;">{len(list1)}</div>
      <div style="font-size:11px;color:#888;margin-top:3px;line-height:1.4;">
        Momentum Gainers<br><span style="color:#aaa;">4%+ &amp; vol ≥100K</span>
      </div>
    </div>
    <div style="text-align:center;min-width:120px;">
      <div style="font-size:30px;font-weight:bold;color:#b71c1c;">{len(list2)}</div>
      <div style="font-size:11px;color:#888;margin-top:3px;line-height:1.4;">
        High-Volume Movers<br><span style="color:#aaa;">4%+ &amp; vol ≥9M</span>
      </div>
    </div>
    <div style="text-align:center;min-width:120px;">
      <div style="font-size:30px;font-weight:bold;color:#5c35a0;">{len(list3)}</div>
      <div style="font-size:11px;color:#888;margin-top:3px;line-height:1.4;">
        Tight Range Stocks<br><span style="color:#aaa;">|change| ≤0.40%</span>
      </div>
    </div>
  </div>
  <div style="background:#fff;border:1px solid #ddd;border-top:none;
              padding:20px 26px 28px;border-radius:0 0 10px 10px;">
    {sec1}
    {sec2}
    {sec3}
    {status_note}
    <hr style="border:none;border-top:1px solid #eee;margin:24px 0 14px;">
    <p style="font-size:11px;color:#bbb;margin:0;line-height:1.6;">
      Data sourced from psxterminal.com. PSX closes 3:30 PM PKT.
      Report runs automatically at 3:35 PM PKT Mon–Fri.<br>
      For informational purposes only — not financial advice.
    </p>
  </div>
</body></html>"""

    return subject, html


# ── Email Sender ───────────────────────────────────────────────────────────────
def send_email(subject, html_body):
    print(f"\n[EMAIL] From: {SENDER_EMAIL}  →  To: {RECIPIENT_EMAIL}")
    if not SENDER_EMAIL:
        print("[EMAIL] ❌ SENDER_EMAIL secret missing!"); sys.exit(1)
    if not SENDER_PASSWORD:
        print("[EMAIL] ❌ SENDER_PASSWORD secret missing!"); sys.exit(1)

    msg            = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SENDER_EMAIL
    msg["To"]      = RECIPIENT_EMAIL
    msg.attach(MIMEText(html_body, "html"))

    print("[EMAIL] Connecting smtp.gmail.com:465...")
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, msg.as_string())
            print(f"[EMAIL] ✅ Sent to {RECIPIENT_EMAIL}")
    except smtplib.SMTPAuthenticationError as e:
        print(f"[EMAIL] ❌ AUTH FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[EMAIL] ❌ ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    scan_date = datetime.now().strftime("%B %d, %Y")
    print(f"\n{'='*62}")
    print(f"  PSX Daily Scanner v6  —  {scan_date}")
    print(f"{'='*62}\n")

    all_stocks, source_name = fetch_all_stocks()

    if not all_stocks:
        print("\n[MAIN] ⚠️  No data retrieved — sending warning email")

    list1, list2, list3 = build_lists(all_stocks) if all_stocks else ([], [], [])

    print(f"\n{'─'*45}")
    print(f"  Total stocks scanned : {len(all_stocks)}")
    print(f"  List 1 Gainers (4%+) : {len(list1)}")
    print(f"  List 2 HiVol  (9M+)  : {len(list2)}")
    print(f"  List 3 Tight Range   : {len(list3)}")
    print(f"{'─'*45}")

    for s in list1[:5]:
        print(f"  📈 {s['symbol']:10s} {s['change_pct']:+.2f}%  vol:{fmt_vol(s['volume'])}")
    for s in list2:
        print(f"  🔥 {s['symbol']:10s} {s['change_pct']:+.2f}%  vol:{fmt_vol(s['volume'])}")

    subject, html = build_email(list1, list2, list3, scan_date, source_name, len(all_stocks))
    print(f"\n[EMAIL] Subject: {subject}")
    send_email(subject, html)
    print(f"\n{'='*62}\n")


if __name__ == "__main__":
    main()
