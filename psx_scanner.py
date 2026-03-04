"""
PSX Daily Stock Scanner — v4
=====================================================================
Runs every weekday at 3:35 PM PKT (10:35 AM UTC) via GitHub Actions.
Scrapes dps.psx.com.pk and emails 3 separate watchlists to jamilnaveed@hotmail.com

LIST 1 — Momentum Gainers   : up 4%+  AND volume >= 100,000
LIST 2 — High-Volume Movers : up 4%+  AND volume >= 9,000,000
LIST 3 — Tight Range Watch  : |change%| <= 0.40% (up OR down)

v4 Fix: Updated to correct PSX endpoint: dps.psx.com.pk/market-watch
        Added multiple fallback URLs for reliability
=====================================================================
"""

import requests
from bs4 import BeautifulSoup
import smtplib
import traceback
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
import os
import sys
import json


# ── Configuration ──────────────────────────────────────────────────────────────
RECIPIENT_EMAIL   = "jamilnaveed@hotmail.com"
SENDER_EMAIL      = os.environ.get("SENDER_EMAIL")
SENDER_PASSWORD   = os.environ.get("SENDER_PASSWORD")

# PSX Endpoints (in order of preference)
PSX_ENDPOINTS = [
    "https://dps.psx.com.pk/market-watch",           # Primary - confirmed working
    "https://dps.psx.com.pk/data/market/equities",   # Fallback 1
    "https://dps.psx.com.pk/equities",                # Fallback 2
]

GAINER_PCT        = 4.0
MIN_VOLUME_L1     = 100_000
MIN_VOLUME_L2     = 9_000_000
TIGHT_RANGE_PCT   = 0.40

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://dps.psx.com.pk/",
    "X-Requested-With": "XMLHttpRequest",
}


# ── Scraper ────────────────────────────────────────────────────────────────────
def parse_stock(item):
    """Safely parse one stock record from various PSX JSON formats."""
    try:
        # Try all known field name variants used by PSX
        symbol     = str(item.get("symbol",  item.get("sym",    item.get("SYMBOL",  "N/A")))).strip()
        name       = str(item.get("name",    item.get("cname",  item.get("CNAME",   symbol)))).strip()
        price      = float(item.get("current",    item.get("ldcp",    item.get("CURRENT",  0))) or 0)
        prev_close = float(item.get("prev_close",  item.get("pclose",  item.get("PCLOSE",   0))) or 0)
        change     = float(item.get("change",      item.get("CHANGE",  0)) or 0)
        change_pct = float(item.get("change_percent", item.get("changep", item.get("CHANGEP", 0))) or 0)
        volume     = int(  item.get("volume",      item.get("vol",     item.get("VOLUME",   0))) or 0)

        # If change_pct is 0 but we have price and prev_close, compute it
        if change_pct == 0 and prev_close > 0 and price > 0:
            change_pct = round(((price - prev_close) / prev_close) * 100, 2)

        return {
            "symbol":     symbol,
            "name":       name,
            "price":      price,
            "prev_close": prev_close,
            "change":     change,
            "change_pct": change_pct,
            "volume":     volume,
        }
    except (ValueError, TypeError):
        return None


def try_json_endpoint(url):
    """Try fetching a URL and parsing as JSON. Returns list of stocks or []."""
    print(f"[SCRAPER] Trying: {url}")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        print(f"[SCRAPER] Status: {resp.status_code}  |  Content-Type: {resp.headers.get('Content-Type','?')}")

        if resp.status_code != 200:
            print(f"[SCRAPER] Non-200 response, skipping.")
            return []

        # Try parsing as JSON
        try:
            data = resp.json()
        except json.JSONDecodeError:
            print(f"[SCRAPER] Response is not JSON, trying HTML parse...")
            return try_html_parse(resp.text)

        # Handle various JSON structures
        raw = []
        if isinstance(data, list):
            raw = data
        elif isinstance(data, dict):
            # Try common wrapper keys
            for key in ["data", "stocks", "equities", "result", "results", "Data", "items"]:
                if key in data and isinstance(data[key], list):
                    raw = data[key]
                    break
            if not raw:
                print(f"[SCRAPER] JSON dict keys: {list(data.keys())}")

        print(f"[SCRAPER] Raw records found: {len(raw)}")
        if not raw:
            return []

        stocks = []
        for item in raw:
            parsed = parse_stock(item)
            if parsed:
                stocks.append(parsed)

        print(f"[SCRAPER] Successfully parsed {len(stocks)} stocks")
        return stocks

    except requests.exceptions.ConnectionError as e:
        print(f"[SCRAPER] Connection error: {e}")
    except requests.exceptions.Timeout:
        print(f"[SCRAPER] Request timed out")
    except Exception as e:
        print(f"[SCRAPER] Unexpected error: {e}")

    return []


def try_html_parse(html_text):
    """Try to extract stock data from an HTML page as last resort."""
    stocks = []
    try:
        soup = BeautifulSoup(html_text, "html.parser")
        rows = soup.select("table tbody tr")
        print(f"[SCRAPER] HTML rows found: {len(rows)}")
        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 5:
                continue
            try:
                change_text = cols[4].get_text(strip=True).replace("%","").replace("+","")
                vol_raw     = cols[5].get_text(strip=True).replace(",","") if len(cols) > 5 else "0"
                stocks.append({
                    "symbol":     cols[0].get_text(strip=True),
                    "name":       cols[1].get_text(strip=True),
                    "price":      float(cols[2].get_text(strip=True).replace(",","")),
                    "prev_close": 0.0,
                    "change":     float(cols[3].get_text(strip=True).replace(",","")),
                    "change_pct": float(change_text),
                    "volume":     int(vol_raw) if vol_raw.isdigit() else 0,
                })
            except (ValueError, IndexError):
                continue
        print(f"[SCRAPER] HTML parse extracted {len(stocks)} stocks")
    except Exception as e:
        print(f"[SCRAPER] HTML parse error: {e}")
    return stocks


def fetch_all_stocks():
    """Try all PSX endpoints until we get data."""
    for url in PSX_ENDPOINTS:
        stocks = try_json_endpoint(url)
        if stocks:
            print(f"[SCRAPER] SUCCESS — {len(stocks)} stocks from {url}")
            return stocks, url
        print(f"[SCRAPER] No data from {url}, trying next...\n")

    print("[SCRAPER] ALL endpoints failed.")
    return [], "all endpoints failed"


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


def sec_header(emoji, title, subtitle, colour):
    return f"""
    <div style="background:{colour};border-radius:8px 8px 0 0;
                padding:13px 18px;margin-top:32px;">
      <h3 style="margin:0;color:#fff;font-size:15.5px;">{emoji}&nbsp; {title}</h3>
      <p style="margin:4px 0 0;color:rgba(255,255,255,0.72);font-size:12px;">{subtitle}</p>
    </div>"""


def empty_msg(text):
    return f'<p style="color:#999;font-style:italic;padding:12px 0 24px;">{text}</p>'


# ── Email Builder ──────────────────────────────────────────────────────────────
def build_email(list1, list2, list3, scan_date, scraper_status, source_url):

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
            sec_header("📈","Momentum Gainers",
                f"Up 4%+ with Volume >= 100,000 &nbsp;|&nbsp; {len(list1)} stock(s) found today","#2e7d32")
            + make_table(rows, ["Symbol","Company","Price (PKR)","Change %","Volume"])
        )
    else:
        sec1 = (
            sec_header("📈","Momentum Gainers","Up 4%+ with Volume >= 100K","#2e7d32")
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
            sec_header("🔥","High-Volume Power Movers",
                f"Up 4%+ with Volume >= 9,000,000 &nbsp;|&nbsp; {len(list2)} stock(s) found today","#b71c1c")
            + make_table(rows, ["Symbol","Company","Price (PKR)","Change %","Volume"])
        )
    else:
        sec2 = (
            sec_header("🔥","High-Volume Power Movers","Up 4%+ with Volume >= 9M","#b71c1c")
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
            sec_header("🔔","Tight Range Watch",
                f"|Change| <= 0.40% (up or down) — Consolidating candidates "
                f"&nbsp;|&nbsp; {len(list3)} stock(s) found today","#5c35a0")
            + make_table(rows, ["Symbol","Company","Price (PKR)","Change %","Direction","Volume"])
        )
    else:
        sec3 = (
            sec_header("🔔","Tight Range Watch","|Change| <= 0.40% (up or down)","#5c35a0")
            + empty_msg("No tight-range consolidating stocks found today.")
        )

    # Status bar
    ok = "success" in scraper_status.lower()
    status_note = f"""
    <div style="background:{'#e8f5e9' if ok else '#fff8e1'};border-radius:6px;
                padding:10px 14px;margin-top:28px;font-size:12px;color:#555;">
      {'✅' if ok else '⚠️'} <strong>Data source:</strong> {scraper_status}
      {'<br>🔗 <strong>URL used:</strong> ' + source_url if ok else ''}
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
      Data sourced from <a href="https://dps.psx.com.pk" style="color:#bbb;">dps.psx.com.pk</a>.
      PSX closes at 3:30 PM PKT. This report runs at 3:35 PM PKT Mon–Fri.<br>
      For informational purposes only — not financial advice.
    </p>
  </div>

</body>
</html>"""

    return subject, html


# ── Email Sender ───────────────────────────────────────────────────────────────
def send_email(subject, html_body):
    print(f"\n[EMAIL] Preparing to send...")
    print(f"[EMAIL] From : {SENDER_EMAIL}")
    print(f"[EMAIL] To   : {RECIPIENT_EMAIL}")

    if not SENDER_EMAIL:
        print("[EMAIL] ERROR: SENDER_EMAIL secret is missing!")
        sys.exit(1)
    if not SENDER_PASSWORD:
        print("[EMAIL] ERROR: SENDER_PASSWORD secret is missing!")
        sys.exit(1)

    msg            = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SENDER_EMAIL
    msg["To"]      = RECIPIENT_EMAIL
    msg.attach(MIMEText(html_body, "html"))

    print("[EMAIL] Connecting to smtp.gmail.com:465...")
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            print("[EMAIL] Connected. Logging in...")
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            print("[EMAIL] Login OK. Sending message...")
            server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, msg.as_string())
            print(f"[EMAIL] ✅ Sent successfully to {RECIPIENT_EMAIL}")
    except smtplib.SMTPAuthenticationError as e:
        print(f"[EMAIL] ❌ AUTH FAILED: {e}")
        print("[EMAIL] → Make sure SENDER_PASSWORD is a Gmail APP PASSWORD (16 chars, no spaces)")
        print("[EMAIL] → Get one at: myaccount.google.com → Security → App Passwords")
        sys.exit(1)
    except Exception as e:
        print(f"[EMAIL] ❌ ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    scan_date = datetime.now().strftime("%B %d, %Y")
    print(f"\n{'='*62}")
    print(f"  PSX Daily Scanner v4  —  {scan_date}")
    print(f"{'='*62}\n")

    all_stocks, source_url = fetch_all_stocks()

    if not all_stocks:
        scraper_status = "WARNING: No stock data retrieved — all PSX endpoints failed"
        print(f"\n[MAIN] {scraper_status}")
        list1, list2, list3 = [], [], []
    else:
        scraper_status = f"Success — {len(all_stocks)} total stocks retrieved"
        list1, list2, list3 = build_lists(all_stocks)

    print(f"\n[LIST 1] Momentum Gainers   (4%+ / vol>=100K) : {len(list1)} stocks")
    for s in list1[:5]:
        print(f"         {s['symbol']:10s}  {s['change_pct']:+.2f}%   vol:{fmt_vol(s['volume'])}")

    print(f"\n[LIST 2] High-Volume Movers (4%+ / vol>=9M)   : {len(list2)} stocks")
    for s in list2:
        print(f"         {s['symbol']:10s}  {s['change_pct']:+.2f}%   vol:{fmt_vol(s['volume'])}")

    print(f"\n[LIST 3] Tight Range Watch  (|pct|<=0.40%)    : {len(list3)} stocks")
    for s in list3[:5]:
        print(f"         {s['symbol']:10s}  {s['change_pct']:+.2f}%   vol:{fmt_vol(s['volume'])}")

    subject, html_body = build_email(list1, list2, list3, scan_date, scraper_status, source_url)
    print(f"\n[MAIN] Subject: {subject}")
    send_email(subject, html_body)
    print(f"\n{'='*62}\n")


if __name__ == "__main__":
    main()
