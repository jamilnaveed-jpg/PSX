"""
PSX Daily Stock Scanner — v7 FINAL
=====================================================================
Data source: psxterminal.com (confirmed working)

Strategy:
  1. GET /api/symbols          → all 553 PSX symbols
  2. GET /api/ticks/REG/{sym}  → price, change, changePercent, volume per symbol

Key field mappings (confirmed from diagnostic):
  price         → current price (PKR)
  change        → absolute change
  changePercent → decimal e.g. 0.02058 = 2.058%  ← multiply by 100
  volume        → shares traded

LIST 1 — Momentum Gainers   : changePercent >= 4%  AND volume >= 100,000
LIST 2 — High-Volume Movers : changePercent >= 4%  AND volume >= 9,000,000
LIST 3 — Tight Range Watch  : |changePercent| <= 0.40% (up OR down)
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

GAINER_PCT      = 4.0      # List 1 & 2: minimum % gain
MIN_VOLUME_L1   = 100_000  # List 1: minimum volume
MIN_VOLUME_L2   = 9_000_000  # List 2: high-volume threshold
TIGHT_RANGE_PCT = 0.40     # List 3: max absolute % change


# ── Step 1: Get all symbols ────────────────────────────────────────────────────
def get_symbols():
    url  = f"{BASE_URL}/api/symbols"
    resp = requests.get(url, headers=HEADERS, timeout=20)
    data = resp.json()
    raw  = data.get("data", data) if isinstance(data, dict) else data
    symbols = [str(s).strip() for s in raw if s]
    print(f"[SYMBOLS] {len(symbols)} symbols fetched")
    return symbols


# ── Step 2: Fetch tick for one symbol ─────────────────────────────────────────
def get_tick(symbol):
    """
    Returns dict with confirmed fields:
      price, change, changePercent (decimal), volume
    changePercent is stored as decimal (e.g. 0.02058)
    We convert to % by multiplying by 100
    """
    url  = f"{BASE_URL}/api/ticks/REG/{symbol}"
    resp = requests.get(url, headers=HEADERS, timeout=10)
    if resp.status_code != 200:
        return None
    data = resp.json()
    tick = data.get("data", {}) if isinstance(data, dict) else {}
    if not tick or not isinstance(tick, dict):
        return None

    price      = float(tick.get("price",         0) or 0)
    change     = float(tick.get("change",         0) or 0)
    change_pct = float(tick.get("changePercent",  0) or 0) * 100  # convert decimal → %
    volume     = int(  tick.get("volume",         0) or 0)
    high       = float(tick.get("high",           0) or 0)
    low        = float(tick.get("low",            0) or 0)

    # Compute prev_close from price and change
    prev_close = round(price - change, 2) if price and change else 0

    return {
        "symbol":     symbol,
        "name":       symbol,   # psxterminal ticks don't return company name
        "price":      price,
        "prev_close": prev_close,
        "change":     change,
        "change_pct": round(change_pct, 2),
        "volume":     volume,
        "high":       high,
        "low":        low,
    }


# ── Step 3: Fetch all ticks ────────────────────────────────────────────────────
def fetch_all_stocks():
    symbols = get_symbols()
    if not symbols:
        return [], "failed to get symbols"

    stocks  = []
    errors  = 0
    total   = len(symbols)

    print(f"[TICKS] Fetching {total} symbols...")

    for i, sym in enumerate(symbols):
        try:
            tick = get_tick(sym)
            if tick and tick["price"] > 0:
                stocks.append(tick)
        except Exception:
            errors += 1

        # Progress log every 100 symbols
        if (i + 1) % 100 == 0:
            print(f"[TICKS] {i+1}/{total} done — {len(stocks)} valid, {errors} errors")

        # Small polite delay every 50 requests
        if (i + 1) % 50 == 0:
            time.sleep(0.3)

    print(f"[TICKS] Complete — {len(stocks)} stocks with price data, {errors} errors")

    # Log a few samples so we can verify data looks correct
    sample = sorted(stocks, key=lambda x: abs(x["change_pct"]), reverse=True)[:5]
    print(f"[TICKS] Top movers sample:")
    for s in sample:
        direction = "▲" if s["change_pct"] > 0 else "▼"
        print(f"  {s['symbol']:10s}  {direction} {s['change_pct']:+.2f}%  price:{s['price']:.2f}  vol:{fmt_vol(s['volume'])}")

    return stocks, "psxterminal.com/api/ticks/REG"


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
        align = "left" if i == 0 else "right"
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

    # ── Section 1: Momentum Gainers ──────────────────────────────────────────
    if list1:
        rows = ""
        for i, s in enumerate(list1):
            bg = "#f9fafb" if i % 2 == 0 else "#fff"
            rows += f"""<tr style="background:{bg};">
              <td style="padding:9px 13px;font-weight:bold;color:#1a1a2e;">{s['symbol']}</td>
              <td style="padding:9px 13px;text-align:right;">{s['price']:,.2f}</td>
              <td style="padding:9px 13px;text-align:right;color:#2e7d32;font-weight:bold;">
                  +{s['change']:,.2f}</td>
              <td style="padding:9px 13px;text-align:right;">{pct_badge(s['change_pct'])}</td>
              <td style="padding:9px 13px;text-align:right;">{s['high']:,.2f}</td>
              <td style="padding:9px 13px;text-align:right;">{s['low']:,.2f}</td>
              <td style="padding:9px 13px;text-align:right;color:#555;">{fmt_vol(s['volume'])}</td>
            </tr>"""
        sec1 = (
            sec_hdr("📈","Momentum Gainers",
                f"Up 4%+ with Volume >= 100,000 &nbsp;|&nbsp; {len(list1)} stock(s) found today","#2e7d32")
            + make_table(rows, ["Symbol","Price (PKR)","Change","Change %","High","Low","Volume"])
        )
    else:
        sec1 = (
            sec_hdr("📈","Momentum Gainers","Up 4%+ with Volume >= 100K","#2e7d32")
            + empty_msg("No stocks met the Momentum Gainer criteria today.")
        )

    # ── Section 2: High-Volume Power Movers ──────────────────────────────────
    if list2:
        rows = ""
        for i, s in enumerate(list2):
            bg = "#fdf4f4" if i % 2 == 0 else "#fff"
            rows += f"""<tr style="background:{bg};">
              <td style="padding:9px 13px;font-weight:bold;color:#1a1a2e;">{s['symbol']}</td>
              <td style="padding:9px 13px;text-align:right;">{s['price']:,.2f}</td>
              <td style="padding:9px 13px;text-align:right;color:#2e7d32;font-weight:bold;">
                  +{s['change']:,.2f}</td>
              <td style="padding:9px 13px;text-align:right;">{pct_badge(s['change_pct'])}</td>
              <td style="padding:9px 13px;text-align:right;">{s['high']:,.2f}</td>
              <td style="padding:9px 13px;text-align:right;">{s['low']:,.2f}</td>
              <td style="padding:9px 13px;text-align:right;font-weight:bold;color:#b71c1c;">
                  {fmt_vol(s['volume'])}</td>
            </tr>"""
        sec2 = (
            sec_hdr("🔥","High-Volume Power Movers",
                f"Up 4%+ with Volume >= 9,000,000 &nbsp;|&nbsp; {len(list2)} stock(s) found today","#b71c1c")
            + make_table(rows, ["Symbol","Price (PKR)","Change","Change %","High","Low","Volume"])
        )
    else:
        sec2 = (
            sec_hdr("🔥","High-Volume Power Movers","Up 4%+ with Volume >= 9M","#b71c1c")
            + empty_msg("No stocks traded above 9M volume with a 4%+ gain today.")
        )

    # ── Section 3: Tight Range Watch ─────────────────────────────────────────
    if list3:
        rows = ""
        for i, s in enumerate(list3):
            bg = "#f7f4ff" if i % 2 == 0 else "#fff"
            if s['change_pct'] > 0:
                direction = '<span style="color:#2e7d32;font-weight:bold;">▲ Up</span>'
            elif s['change_pct'] < 0:
                direction = '<span style="color:#c62828;font-weight:bold;">▼ Down</span>'
            else:
                direction = '<span style="color:#888;">— Flat</span>'
            rows += f"""<tr style="background:{bg};">
              <td style="padding:9px 13px;font-weight:bold;color:#1a1a2e;">{s['symbol']}</td>
              <td style="padding:9px 13px;text-align:right;">{s['price']:,.2f}</td>
              <td style="padding:9px 13px;text-align:right;">{pct_badge(s['change_pct'])}</td>
              <td style="padding:9px 13px;text-align:center;">{direction}</td>
              <td style="padding:9px 13px;text-align:right;">{s['high']:,.2f}</td>
              <td style="padding:9px 13px;text-align:right;">{s['low']:,.2f}</td>
              <td style="padding:9px 13px;text-align:right;color:#666;">{fmt_vol(s['volume'])}</td>
            </tr>"""
        sec3 = (
            sec_hdr("🔔","Tight Range Watch",
                f"|Change| <= 0.40% (up or down) — Consolidating candidates "
                f"&nbsp;|&nbsp; {len(list3)} stock(s) found today","#5c35a0")
            + make_table(rows, ["Symbol","Price (PKR)","Change %","Direction","High","Low","Volume"])
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
<body style="font-family:Arial,sans-serif;max-width:900px;margin:auto;
             padding:20px;background:#f0f0f0;">

  <div style="background:#1a1a2e;padding:22px 26px;border-radius:10px 10px 0 0;">
    <h1 style="color:#fff;margin:0;font-size:22px;">📊 PSX Daily Stock Scanner</h1>
    <p style="color:#aaa;margin:6px 0 0;font-size:13px;">
      {scan_date} &nbsp;|&nbsp; End-of-Day Report &nbsp;|&nbsp; Pakistan Stock Exchange
    </p>
  </div>

  <!-- Summary bar -->
  <div style="background:#fff;border:1px solid #ddd;border-top:none;
              padding:18px 26px;display:flex;gap:40px;flex-wrap:wrap;">
    <div style="text-align:center;min-width:110px;">
      <div style="font-size:30px;font-weight:bold;color:#2e7d32;">{len(list1)}</div>
      <div style="font-size:11px;color:#888;margin-top:3px;line-height:1.4;">
        Momentum Gainers<br><span style="color:#aaa;">4%+ &amp; vol ≥100K</span></div>
    </div>
    <div style="text-align:center;min-width:110px;">
      <div style="font-size:30px;font-weight:bold;color:#b71c1c;">{len(list2)}</div>
      <div style="font-size:11px;color:#888;margin-top:3px;line-height:1.4;">
        High-Volume Movers<br><span style="color:#aaa;">4%+ &amp; vol ≥9M</span></div>
    </div>
    <div style="text-align:center;min-width:110px;">
      <div style="font-size:30px;font-weight:bold;color:#5c35a0;">{len(list3)}</div>
      <div style="font-size:11px;color:#888;margin-top:3px;line-height:1.4;">
        Tight Range Stocks<br><span style="color:#aaa;">|change| ≤0.40%</span></div>
    </div>
    <div style="text-align:center;min-width:110px;">
      <div style="font-size:30px;font-weight:bold;color:#555;">{total_stocks}</div>
      <div style="font-size:11px;color:#888;margin-top:3px;line-height:1.4;">
        Total Scanned<br><span style="color:#aaa;">all PSX stocks</span></div>
    </div>
  </div>

  <!-- Main content -->
  <div style="background:#fff;border:1px solid #ddd;border-top:none;
              padding:20px 26px 28px;border-radius:0 0 10px 10px;">
    {sec1}
    {sec2}
    {sec3}
    {status_note}
    <hr style="border:none;border-top:1px solid #eee;margin:24px 0 14px;">
    <p style="font-size:11px;color:#bbb;margin:0;line-height:1.6;">
      Data sourced from <a href="https://psxterminal.com" style="color:#bbb;">psxterminal.com</a>.
      PSX closes 3:30 PM PKT. Report runs at 3:35 PM PKT Mon–Fri.<br>
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

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, msg.as_string())
            print(f"[EMAIL] ✅ Sent successfully to {RECIPIENT_EMAIL}")
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
    print(f"  PSX Daily Scanner v7  —  {scan_date}")
    print(f"{'='*62}\n")

    all_stocks, source_name = fetch_all_stocks()

    if not all_stocks:
        print("\n[MAIN] ⚠️ No data — sending warning email")

    list1, list2, list3 = build_lists(all_stocks) if all_stocks else ([], [], [])

    print(f"\n{'─'*45}")
    print(f"  Total stocks scanned  : {len(all_stocks)}")
    print(f"  List 1 Gainers  4%+   : {len(list1)}")
    print(f"  List 2 HiVol    9M+   : {len(list2)}")
    print(f"  List 3 TightRange     : {len(list3)}")
    print(f"{'─'*45}")

    if list1:
        print("\n  📈 TOP GAINERS:")
        for s in list1[:10]:
            print(f"     {s['symbol']:10s}  {s['change_pct']:+.2f}%  price:{s['price']:.2f}  vol:{fmt_vol(s['volume'])}")
    if list2:
        print("\n  🔥 HIGH VOLUME:")
        for s in list2:
            print(f"     {s['symbol']:10s}  {s['change_pct']:+.2f}%  price:{s['price']:.2f}  vol:{fmt_vol(s['volume'])}")

    subject, html = build_email(list1, list2, list3, scan_date, source_name, len(all_stocks))
    print(f"\n[EMAIL] Subject: {subject}")
    send_email(subject, html)
    print(f"\n{'='*62}\n")


if __name__ == "__main__":
    main()
