"""
PSX Daily Stock Scanner — v5
=====================================================================
Runs every weekday at 3:35 PM PKT (10:35 AM UTC) via GitHub Actions.
Emails 3 watchlists to jamilnaveed@hotmail.com

LIST 1 — Momentum Gainers   : up 4%+  AND volume >= 100,000
LIST 2 — High-Volume Movers : up 4%+  AND volume >= 9,000,000
LIST 3 — Tight Range Watch  : |change%| <= 0.40% (up OR down)

v5: Switched to psxterminal.com API (reliable, free, no auth needed)
    + dps.psx.com.pk/market-watch as secondary
    + dps.psx.com.pk/sector-summary as tertiary
    + Full debug logging at every step
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
RECIPIENT_EMAIL  = "jamilnaveed@hotmail.com"
SENDER_EMAIL     = os.environ.get("SENDER_EMAIL")
SENDER_PASSWORD  = os.environ.get("SENDER_PASSWORD")

GAINER_PCT       = 4.0
MIN_VOLUME_L1    = 100_000
MIN_VOLUME_L2    = 9_000_000
TIGHT_RANGE_PCT  = 0.40

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept":           "application/json, text/html, */*",
    "Accept-Language":  "en-US,en;q=0.9",
    "Referer":          "https://psxterminal.com/",
}


# ── Source 1: psxterminal.com (most reliable, free, open) ─────────────────────
def fetch_psxterminal():
    """
    psxterminal.com provides a free REST API for PSX.
    Endpoint: https://psxterminal.com/api/stats/REG
    Returns all regular market stocks with price, change, volume.
    """
    url = "https://psxterminal.com/api/stats/REG"
    print(f"\n[SOURCE 1] psxterminal.com → {url}")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        print(f"[SOURCE 1] Status: {resp.status_code}")
        print(f"[SOURCE 1] Content-Type: {resp.headers.get('Content-Type','?')}")
        print(f"[SOURCE 1] Response size: {len(resp.content)} bytes")

        if resp.status_code != 200:
            print(f"[SOURCE 1] Non-200, skipping.")
            return []

        data = resp.json()
        print(f"[SOURCE 1] JSON type: {type(data).__name__}")

        # Log a sample to understand structure
        if isinstance(data, list) and len(data) > 0:
            print(f"[SOURCE 1] Sample record keys: {list(data[0].keys()) if isinstance(data[0], dict) else 'not a dict'}")
        elif isinstance(data, dict):
            print(f"[SOURCE 1] Top-level keys: {list(data.keys())}")

        raw = data if isinstance(data, list) else data.get("data", data.get("stocks", data.get("result", [])))
        print(f"[SOURCE 1] Raw records: {len(raw)}")

        stocks = []
        for item in raw:
            try:
                # psxterminal field names (from their API docs)
                symbol     = str(item.get("symbol",  item.get("sym",    "N/A"))).strip()
                name       = str(item.get("name",    item.get("company", symbol))).strip()
                price      = float(item.get("last",      item.get("current", item.get("close",  0))) or 0)
                prev_close = float(item.get("prevClose", item.get("prev",    item.get("pclose", 0))) or 0)
                change     = float(item.get("change",    0) or 0)
                change_pct = float(item.get("changePct", item.get("change_pct", item.get("changep", 0))) or 0)
                volume     = int(  item.get("volume",    item.get("vol",  0)) or 0)

                # Compute change_pct if missing but we have prices
                if change_pct == 0 and prev_close > 0 and price > 0:
                    change_pct = round(((price - prev_close) / prev_close) * 100, 2)

                if symbol != "N/A":
                    stocks.append({
                        "symbol": symbol, "name": name,
                        "price": price, "prev_close": prev_close,
                        "change": change, "change_pct": change_pct,
                        "volume": volume,
                    })
            except (ValueError, TypeError):
                continue

        print(f"[SOURCE 1] Parsed: {len(stocks)} stocks")
        if stocks:
            sample = stocks[:3]
            for s in sample:
                print(f"[SOURCE 1] Sample: {s['symbol']} price={s['price']} chg={s['change_pct']}% vol={s['volume']}")
        return stocks

    except Exception as e:
        print(f"[SOURCE 1] FAILED: {e}")
        traceback.print_exc()
        return []


# ── Source 2: dps.psx.com.pk/market-watch ─────────────────────────────────────
def fetch_dps_market_watch():
    """PSX official market-watch endpoint."""
    url = "https://dps.psx.com.pk/market-watch"
    print(f"\n[SOURCE 2] dps.psx.com.pk → {url}")
    try:
        hdrs = dict(HEADERS)
        hdrs["Referer"] = "https://dps.psx.com.pk/"
        resp = requests.get(url, headers=hdrs, timeout=20)
        print(f"[SOURCE 2] Status: {resp.status_code}")
        print(f"[SOURCE 2] Content-Type: {resp.headers.get('Content-Type','?')}")
        print(f"[SOURCE 2] Response size: {len(resp.content)} bytes")

        if resp.status_code != 200:
            print(f"[SOURCE 2] Non-200, skipping.")
            return []

        # Try JSON first
        try:
            data = resp.json()
            print(f"[SOURCE 2] Got JSON, type: {type(data).__name__}")
            if isinstance(data, dict):
                print(f"[SOURCE 2] Keys: {list(data.keys())}")
            raw = data if isinstance(data, list) else data.get("data", data.get("stocks", []))
            stocks = []
            for item in raw:
                try:
                    vol = int(item.get("volume", item.get("vol", 0)) or 0)
                    pct = float(item.get("change_percent", item.get("changep", item.get("ldcp", 0))) or 0)
                    price = float(item.get("current", item.get("ldcp", 0)) or 0)
                    prev  = float(item.get("prev_close", item.get("pclose", 0)) or 0)
                    if pct == 0 and prev > 0 and price > 0:
                        pct = round(((price - prev) / prev) * 100, 2)
                    stocks.append({
                        "symbol":     str(item.get("symbol", item.get("sym", "N/A"))).strip(),
                        "name":       str(item.get("name",   item.get("cname", "N/A"))).strip(),
                        "price":      price,
                        "prev_close": prev,
                        "change":     float(item.get("change", 0) or 0),
                        "change_pct": pct,
                        "volume":     vol,
                    })
                except (ValueError, TypeError):
                    continue
            print(f"[SOURCE 2] Parsed: {len(stocks)} stocks")
            return stocks
        except json.JSONDecodeError:
            print(f"[SOURCE 2] Not JSON — trying HTML parse")
            return parse_html_table(resp.text, source="SOURCE 2")

    except Exception as e:
        print(f"[SOURCE 2] FAILED: {e}")
        return []


# ── Source 3: dps.psx.com.pk/sector-summary ───────────────────────────────────
def fetch_dps_sector():
    """Fallback: PSX sector summary page."""
    url = "https://dps.psx.com.pk/sector-summary"
    print(f"\n[SOURCE 3] dps.psx.com.pk/sector-summary → {url}")
    try:
        hdrs = dict(HEADERS)
        hdrs["Referer"] = "https://dps.psx.com.pk/"
        resp = requests.get(url, headers=hdrs, timeout=20)
        print(f"[SOURCE 3] Status: {resp.status_code}")
        if resp.status_code != 200:
            return []
        try:
            data = resp.json()
            raw  = data if isinstance(data, list) else data.get("data", [])
            stocks = []
            for item in raw:
                try:
                    stocks.append({
                        "symbol":     str(item.get("symbol", "N/A")).strip(),
                        "name":       str(item.get("name",   "N/A")).strip(),
                        "price":      float(item.get("current", 0) or 0),
                        "prev_close": float(item.get("prev_close", 0) or 0),
                        "change":     float(item.get("change", 0) or 0),
                        "change_pct": float(item.get("change_percent", 0) or 0),
                        "volume":     int(item.get("volume", 0) or 0),
                    })
                except (ValueError, TypeError):
                    continue
            print(f"[SOURCE 3] Parsed: {len(stocks)} stocks")
            return stocks
        except json.JSONDecodeError:
            return parse_html_table(resp.text, source="SOURCE 3")
    except Exception as e:
        print(f"[SOURCE 3] FAILED: {e}")
        return []


# ── Source 4: psxterminal.com/api/ticks/REG (alternative endpoint) ────────────
def fetch_psxterminal_ticks():
    """Alternative psxterminal endpoint — returns live tick data for all stocks."""
    url = "https://psxterminal.com/api/ticks/REG"
    print(f"\n[SOURCE 4] psxterminal ticks → {url}")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=25)
        print(f"[SOURCE 4] Status: {resp.status_code}, Size: {len(resp.content)} bytes")
        if resp.status_code != 200:
            return []
        data = resp.json()
        raw  = data if isinstance(data, list) else data.get("data", data.get("ticks", []))
        stocks = []
        for item in raw:
            try:
                price = float(item.get("last", item.get("ltp", item.get("close", 0))) or 0)
                prev  = float(item.get("prevClose", item.get("prev", 0)) or 0)
                pct   = float(item.get("changePct", item.get("change_pct", 0)) or 0)
                if pct == 0 and prev > 0 and price > 0:
                    pct = round(((price - prev) / prev) * 100, 2)
                stocks.append({
                    "symbol":     str(item.get("symbol", "N/A")).strip(),
                    "name":       str(item.get("name", item.get("company", "N/A"))).strip(),
                    "price":      price,
                    "prev_close": prev,
                    "change":     float(item.get("change", 0) or 0),
                    "change_pct": pct,
                    "volume":     int(item.get("volume", item.get("vol", 0)) or 0),
                })
            except (ValueError, TypeError):
                continue
        print(f"[SOURCE 4] Parsed: {len(stocks)} stocks")
        return stocks
    except Exception as e:
        print(f"[SOURCE 4] FAILED: {e}")
        return []


def parse_html_table(html_text, source="HTML"):
    """Generic HTML table parser fallback."""
    stocks = []
    try:
        soup = BeautifulSoup(html_text, "html.parser")
        rows = soup.select("table tbody tr")
        print(f"[{source}] HTML rows: {len(rows)}")
        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 5:
                continue
            try:
                chg_txt = cols[4].get_text(strip=True).replace("%","").replace("+","")
                vol_txt = cols[5].get_text(strip=True).replace(",","") if len(cols) > 5 else "0"
                stocks.append({
                    "symbol":     cols[0].get_text(strip=True),
                    "name":       cols[1].get_text(strip=True),
                    "price":      float(cols[2].get_text(strip=True).replace(",","")),
                    "prev_close": 0.0,
                    "change":     float(cols[3].get_text(strip=True).replace(",","")),
                    "change_pct": float(chg_txt),
                    "volume":     int(vol_txt) if vol_txt.isdigit() else 0,
                })
            except (ValueError, IndexError):
                continue
        print(f"[{source}] HTML parsed: {len(stocks)} stocks")
    except Exception as e:
        print(f"[{source}] HTML parse error: {e}")
    return stocks


# ── Master fetch: try all sources in order ─────────────────────────────────────
def fetch_all_stocks():
    sources = [
        ("psxterminal.com/api/stats",   fetch_psxterminal),
        ("psxterminal.com/api/ticks",   fetch_psxterminal_ticks),
        ("dps.psx.com.pk/market-watch", fetch_dps_market_watch),
        ("dps.psx.com.pk/sector-summary", fetch_dps_sector),
    ]

    for name, fn in sources:
        print(f"\n{'─'*50}")
        print(f"Trying source: {name}")
        print(f"{'─'*50}")
        try:
            stocks = fn()
            if stocks and len(stocks) > 5:
                print(f"\n✅ SUCCESS — {len(stocks)} stocks from [{name}]")
                return stocks, name
            else:
                print(f"⚠️  Source [{name}] returned {len(stocks)} stocks — trying next...")
        except Exception as e:
            print(f"❌ Source [{name}] threw exception: {e}")

    print("\n❌ ALL SOURCES FAILED")
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

    def make_rows(lst, include_direction=False):
        rows = ""
        for i, s in enumerate(lst):
            bg = "#f9fafb" if i % 2 == 0 else "#fff"
            if include_direction:
                if s['change_pct'] > 0:
                    d = '<span style="color:#2e7d32;">▲ Up</span>'
                elif s['change_pct'] < 0:
                    d = '<span style="color:#c62828;">▼ Down</span>'
                else:
                    d = '<span style="color:#888;">— Flat</span>'
                rows += f"""<tr style="background:{bg};">
                  <td style="padding:9px 13px;font-weight:bold;color:#1a1a2e;">{s['symbol']}</td>
                  <td style="padding:9px 13px;color:#444;">{s['name']}</td>
                  <td style="padding:9px 13px;text-align:right;">{s['price']:,.2f}</td>
                  <td style="padding:9px 13px;text-align:right;">{pct_badge(s['change_pct'])}</td>
                  <td style="padding:9px 13px;text-align:center;">{d}</td>
                  <td style="padding:9px 13px;text-align:right;color:#666;">{fmt_vol(s['volume'])}</td>
                </tr>"""
            else:
                rows += f"""<tr style="background:{bg};">
                  <td style="padding:9px 13px;font-weight:bold;color:#1a1a2e;">{s['symbol']}</td>
                  <td style="padding:9px 13px;color:#444;">{s['name']}</td>
                  <td style="padding:9px 13px;text-align:right;">{s['price']:,.2f}</td>
                  <td style="padding:9px 13px;text-align:right;">{pct_badge(s['change_pct'])}</td>
                  <td style="padding:9px 13px;text-align:right;color:#555;">{fmt_vol(s['volume'])}</td>
                </tr>"""
        return rows

    # Section 1
    if list1:
        sec1 = (sec_hdr("📈","Momentum Gainers",
                    f"Up 4%+ with Volume >= 100,000 &nbsp;|&nbsp; {len(list1)} stock(s) found","#2e7d32")
                + make_table(make_rows(list1), ["Symbol","Company","Price (PKR)","Change %","Volume"]))
    else:
        sec1 = (sec_hdr("📈","Momentum Gainers","Up 4%+ with Volume >= 100K","#2e7d32")
                + empty_msg("No stocks met the Momentum Gainer criteria today."))

    # Section 2
    if list2:
        rows2 = ""
        for i, s in enumerate(list2):
            bg = "#fdf4f4" if i % 2 == 0 else "#fff"
            rows2 += f"""<tr style="background:{bg};">
              <td style="padding:9px 13px;font-weight:bold;color:#1a1a2e;">{s['symbol']}</td>
              <td style="padding:9px 13px;color:#444;">{s['name']}</td>
              <td style="padding:9px 13px;text-align:right;">{s['price']:,.2f}</td>
              <td style="padding:9px 13px;text-align:right;">{pct_badge(s['change_pct'])}</td>
              <td style="padding:9px 13px;text-align:right;font-weight:bold;color:#b71c1c;">{fmt_vol(s['volume'])}</td>
            </tr>"""
        sec2 = (sec_hdr("🔥","High-Volume Power Movers",
                    f"Up 4%+ with Volume >= 9,000,000 &nbsp;|&nbsp; {len(list2)} stock(s) found","#b71c1c")
                + make_table(rows2, ["Symbol","Company","Price (PKR)","Change %","Volume"]))
    else:
        sec2 = (sec_hdr("🔥","High-Volume Power Movers","Up 4%+ with Volume >= 9M","#b71c1c")
                + empty_msg("No stocks traded above 9M volume with a 4%+ gain today."))

    # Section 3
    if list3:
        sec3 = (sec_hdr("🔔","Tight Range Watch",
                    f"|Change| <= 0.40% (up or down) &nbsp;|&nbsp; {len(list3)} stock(s) found","#5c35a0")
                + make_table(make_rows(list3, include_direction=True),
                             ["Symbol","Company","Price (PKR)","Change %","Direction","Volume"]))
    else:
        sec3 = (sec_hdr("🔔","Tight Range Watch","|Change| <= 0.40% (up or down)","#5c35a0")
                + empty_msg("No tight-range consolidating stocks found today."))

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
      PSX closes 3:30 PM PKT. Report runs 3:35 PM PKT Mon–Fri.
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
        print("[EMAIL]    → Use Gmail App Password (16 chars) not your regular password")
        print("[EMAIL]    → myaccount.google.com → Security → App Passwords")
        sys.exit(1)
    except Exception as e:
        print(f"[EMAIL] ❌ ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    scan_date = datetime.now().strftime("%B %d, %Y")
    print(f"\n{'='*62}")
    print(f"  PSX Daily Scanner v5  —  {scan_date}")
    print(f"{'='*62}")

    all_stocks, source_name = fetch_all_stocks()

    if not all_stocks:
        print("\n[MAIN] No data from any source — sending warning email")
        source_name = "⚠️ All sources failed — no PSX data today"

    list1, list2, list3 = build_lists(all_stocks) if all_stocks else ([], [], [])

    print(f"\n{'─'*40}")
    print(f"[RESULTS] Total stocks scanned : {len(all_stocks)}")
    print(f"[RESULTS] List 1 Momentum      : {len(list1)}")
    print(f"[RESULTS] List 2 High Volume   : {len(list2)}")
    print(f"[RESULTS] List 3 Tight Range   : {len(list3)}")
    print(f"{'─'*40}")

    for s in list1[:5]:
        print(f"  L1: {s['symbol']:10s} {s['change_pct']:+.2f}%  vol:{fmt_vol(s['volume'])}")
    for s in list2:
        print(f"  L2: {s['symbol']:10s} {s['change_pct']:+.2f}%  vol:{fmt_vol(s['volume'])}")

    subject, html = build_email(list1, list2, list3, scan_date, source_name, len(all_stocks))
    print(f"\n[EMAIL] Subject: {subject}")
    send_email(subject, html)
    print(f"\n{'='*62}\n")


if __name__ == "__main__":
    main()

