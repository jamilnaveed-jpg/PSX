"""
PSX Trend Scanner — v1
=====================================================================
A SEPARATE daily email sent at PSX market close (3:35 PM PKT).
Uses psxterminal.com /api/klines/{symbol}/1d endpoint for historical
daily candles to compute multi-day price moves.

MINIMUM VOLUME: 100,000 average daily volume over the lookback period

SIX LISTS:
  1  🚀 Up 50%+ in last 20 days   (Bullish — strong uptrend)
  2  💥 Down 50%+ in last 20 days  (Bearish — strong downtrend)
  3  📈 Up 20%+ in last 5 days     (Bullish — short-term momentum)
  4  📉 Down 20%+ in last 5 days   (Bearish — short-term breakdown)
  5  💰 Up PKR 20+ in last 5 days  (Bullish — absolute price gain)
  6  🔻 Down PKR 20+ in last 5 days (Bearish — absolute price loss)

Each list shows: symbol, sector, price 20/5 days ago, current price,
% change, PKR change, avg daily volume, and today's volume.
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

MIN_AVG_VOLUME   = 100_000   # minimum avg daily volume over lookback period
UP_50_DAYS       = 20        # lookback for 50% move
UP_20_DAYS       = 5         # lookback for 20% move and PKR 20 move
PCT_50           = 50.0
PCT_20           = 20.0
PKR_20           = 20.0


# ── Step 1: Get all symbols ────────────────────────────────────────────────────
def get_symbols():
    url  = f"{BASE_URL}/api/symbols"
    resp = requests.get(url, headers=HEADERS, timeout=20)
    data = resp.json()
    raw  = data.get("data", data) if isinstance(data, dict) else data
    symbols = [str(s).strip() for s in raw if s]
    print(f"[SYMBOLS] {len(symbols)} symbols fetched")
    return symbols


# ── Step 2: Get sector map ─────────────────────────────────────────────────────
def get_sector_map(symbols):
    sector_map = {}
    errors = 0
    print(f"[SECTORS] Fetching sector data...")

    for i, sym in enumerate(symbols):
        try:
            url  = f"{BASE_URL}/api/companies/{sym}"
            resp = requests.get(url, headers=HEADERS, timeout=8)
            if resp.status_code == 200:
                data = resp.json()
                info = data.get("data", {}) if isinstance(data, dict) else {}
                sector = (
                    info.get("sector") or info.get("sectorName")
                    or info.get("industry") or "—"
                ) if isinstance(info, dict) else "—"
                sector_map[sym] = str(sector).strip() or "—"
            else:
                sector_map[sym] = "—"
        except Exception:
            sector_map[sym] = "—"
            errors += 1

        if (i + 1) % 100 == 0:
            print(f"[SECTORS] {i+1}/{len(symbols)}, errors:{errors}")
        if (i + 1) % 50 == 0:
            time.sleep(0.2)

    filled = sum(1 for v in sector_map.values() if v != "—")
    print(f"[SECTORS] Done — {filled}/{len(symbols)} sectors found")
    return sector_map


# ── Step 3: Fetch daily klines for one symbol ──────────────────────────────────
def get_klines(symbol, limit=25):
    """
    GET /api/klines/{symbol}/1d?limit=N
    Returns list of daily candles newest-first or oldest-first.
    Each candle: { open, high, low, close, volume, timestamp/time }

    We request 25 candles to cover 20 trading days plus buffer for
    weekends/holidays. Returns list sorted oldest → newest.
    """
    url  = f"{BASE_URL}/api/klines/{symbol}/1d?limit={limit}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=12)
        if resp.status_code != 200:
            return None
        data = resp.json()

        # Handle response structure variations
        raw = data
        if isinstance(data, dict):
            raw = (data.get("data") or data.get("candles") or
                   data.get("klines") or data.get("result") or [])

        if not isinstance(raw, list) or len(raw) < 2:
            return None

        candles = []
        for c in raw:
            if not isinstance(c, dict):
                continue
            # Field name variations
            close  = float(c.get("close",  c.get("c", c.get("Close",  0))) or 0)
            open_  = float(c.get("open",   c.get("o", c.get("Open",   0))) or 0)
            volume = int(  c.get("volume", c.get("v", c.get("Volume", 0))) or 0)
            ts     = c.get("timestamp", c.get("time", c.get("t", c.get("date", 0))))
            if close > 0:
                candles.append({
                    "close":  close,
                    "open":   open_,
                    "volume": volume,
                    "ts":     ts,
                })

        if len(candles) < 2:
            return None

        # Sort by timestamp ascending (oldest first) so index [-1] = today
        try:
            candles.sort(key=lambda x: x["ts"])
        except Exception:
            pass  # if timestamps aren't sortable leave as-is

        return candles

    except Exception as e:
        return None


# ── Step 4: Analyse one symbol ─────────────────────────────────────────────────
def analyse_symbol(symbol, sector):
    """
    Returns a dict with all computed metrics, or None if insufficient data.
    """
    candles = get_klines(symbol, limit=25)
    if not candles or len(candles) < 6:
        return None

    today_price  = candles[-1]["close"]
    today_volume = candles[-1]["volume"]

    if today_price <= 0:
        return None

    # ── 5-day lookback ─────────────────────────────────────────────────────
    # Use close from 5 candles ago (index -6 gives price before 5 sessions)
    if len(candles) >= 6:
        price_5d_ago   = candles[-6]["close"]
        last_5         = candles[-5:]
        avg_vol_5d     = sum(c["volume"] for c in last_5) / len(last_5)
        pct_5d         = round((today_price - price_5d_ago) / price_5d_ago * 100, 2) if price_5d_ago > 0 else 0
        pkr_5d         = round(today_price - price_5d_ago, 2)
    else:
        price_5d_ago = avg_vol_5d = pct_5d = pkr_5d = 0

    # ── 20-day lookback ────────────────────────────────────────────────────
    if len(candles) >= 21:
        price_20d_ago  = candles[-21]["close"]
        last_20        = candles[-20:]
        avg_vol_20d    = sum(c["volume"] for c in last_20) / len(last_20)
        pct_20d        = round((today_price - price_20d_ago) / price_20d_ago * 100, 2) if price_20d_ago > 0 else 0
        pkr_20d        = round(today_price - price_20d_ago, 2)
    elif len(candles) >= 6:
        # Fewer than 21 candles available — use what we have
        price_20d_ago  = candles[0]["close"]
        last_20        = candles
        avg_vol_20d    = sum(c["volume"] for c in last_20) / len(last_20)
        pct_20d        = round((today_price - price_20d_ago) / price_20d_ago * 100, 2) if price_20d_ago > 0 else 0
        pkr_20d        = round(today_price - price_20d_ago, 2)
    else:
        price_20d_ago = avg_vol_20d = pct_20d = pkr_20d = 0

    return {
        "symbol":       symbol,
        "sector":       sector,
        "price":        today_price,
        "volume":       today_volume,
        # 5-day metrics
        "price_5d_ago": price_5d_ago,
        "pct_5d":       pct_5d,
        "pkr_5d":       pkr_5d,
        "avg_vol_5d":   avg_vol_5d,
        # 20-day metrics
        "price_20d_ago": price_20d_ago,
        "pct_20d":       pct_20d,
        "pkr_20d":       pkr_20d,
        "avg_vol_20d":   avg_vol_20d,
    }


# ── Step 5: Fetch and analyse all symbols ──────────────────────────────────────
def fetch_trend_data(symbols, sector_map):
    results = []
    errors  = 0
    total   = len(symbols)
    print(f"\n[TREND] Fetching klines for {total} symbols...")

    for i, sym in enumerate(symbols):
        try:
            rec = analyse_symbol(sym, sector_map.get(sym, "—"))
            if rec:
                results.append(rec)
        except Exception:
            errors += 1

        if (i + 1) % 100 == 0:
            print(f"[TREND] {i+1}/{total} done — {len(results)} valid, {errors} errors")
        if (i + 1) % 50 == 0:
            time.sleep(0.3)

    print(f"[TREND] Complete — {len(results)} stocks analysed, {errors} errors")
    return results


# ── Step 6: Build the 6 lists ──────────────────────────────────────────────────
def build_trend_lists(results):
    """
    Apply filters. All lists require avg daily volume >= 100K
    over the relevant lookback window.
    """
    l1, l2, l3, l4, l5, l6 = [], [], [], [], [], []

    for r in results:
        # Volume gates
        vol_ok_20d = r["avg_vol_20d"] >= MIN_AVG_VOLUME
        vol_ok_5d  = r["avg_vol_5d"]  >= MIN_AVG_VOLUME

        # List 1: Up 50%+ in 20 days
        if vol_ok_20d and r["pct_20d"] >= PCT_50:
            l1.append(r)
        # List 2: Down 50%+ in 20 days
        if vol_ok_20d and r["pct_20d"] <= -PCT_50:
            l2.append(r)
        # List 3: Up 20%+ in 5 days
        if vol_ok_5d and r["pct_5d"] >= PCT_20:
            l3.append(r)
        # List 4: Down 20%+ in 5 days
        if vol_ok_5d and r["pct_5d"] <= -PCT_20:
            l4.append(r)
        # List 5: Up PKR 20+ in 5 days
        if vol_ok_5d and r["pkr_5d"] >= PKR_20:
            l5.append(r)
        # List 6: Down PKR 20+ in 5 days
        if vol_ok_5d and r["pkr_5d"] <= -PKR_20:
            l6.append(r)

    # Sort each list
    l1.sort(key=lambda x: x["pct_20d"],  reverse=True)   # biggest gain
    l2.sort(key=lambda x: x["pct_20d"])                   # biggest loss
    l3.sort(key=lambda x: x["pct_5d"],   reverse=True)
    l4.sort(key=lambda x: x["pct_5d"])
    l5.sort(key=lambda x: x["pkr_5d"],   reverse=True)
    l6.sort(key=lambda x: x["pkr_5d"])

    return l1, l2, l3, l4, l5, l6


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
        bg, fg, sign = "#f5f5f5", "#666", ""
    return (
        f'<span style="background:{bg};color:{fg};padding:3px 8px;'
        f'border-radius:10px;font-weight:bold;font-size:12px;">'
        f'{sign}{pct:.2f}%</span>'
    )


def pkr_badge(pkr):
    if pkr > 0:
        col, sign = "#2e7d32", "+"
    elif pkr < 0:
        col, sign = "#c62828", ""
    else:
        col, sign = "#888", ""
    return f'<span style="color:{col};font-weight:bold;">{sign}PKR {pkr:,.2f}</span>'


def sec_hdr(emoji, title, subtitle, colour):
    return f"""
    <div style="background:{colour};border-radius:8px 8px 0 0;
                padding:12px 18px;margin-top:32px;">
      <h3 style="margin:0;color:#fff;font-size:15px;">{emoji} {title}</h3>
      <p style="margin:3px 0 0;color:rgba(255,255,255,0.78);font-size:11.5px;">{subtitle}</p>
    </div>"""


def empty_note(msg):
    return f'<p style="color:#aaa;font-style:italic;padding:10px 0 20px;">{msg}</p>'


def make_table(headers, rows_html):
    ths = ""
    for i, h in enumerate(headers):
        al = "left" if i <= 1 else "right"
        ths += f'<th style="padding:9px 11px;text-align:{al};font-weight:600;white-space:nowrap;">{h}</th>'
    return f"""
    <table style="width:100%;border-collapse:collapse;font-size:12.5px;
                  font-family:Arial,sans-serif;border:1px solid #e0e0e0;
                  border-radius:6px;overflow:hidden;margin-bottom:4px;">
      <thead><tr style="background:#1a1a2e;color:#fff;">{ths}</tr></thead>
      <tbody>{rows_html}</tbody>
    </table>"""


def rows_20d(lst, bullish=True):
    """Table rows for 20-day lists."""
    html = ""
    for i, r in enumerate(lst):
        bg  = "#f9fafb" if i % 2 == 0 else "#fff"
        chg_col = "#2e7d32" if bullish else "#c62828"
        html += f"""<tr style="background:{bg};">
          <td style="padding:8px 11px;font-weight:bold;color:#1a1a2e;">{r['symbol']}</td>
          <td style="padding:8px 11px;color:#666;font-size:11.5px;">{r['sector']}</td>
          <td style="padding:8px 11px;text-align:right;color:#555;">{r['price_20d_ago']:,.2f}</td>
          <td style="padding:8px 11px;text-align:right;font-weight:bold;">{r['price']:,.2f}</td>
          <td style="padding:8px 11px;text-align:right;font-weight:bold;color:{chg_col};">
              {'+'if bullish else ''}{r['pkr_20d']:,.2f}</td>
          <td style="padding:8px 11px;text-align:right;">{pct_badge(r['pct_20d'])}</td>
          <td style="padding:8px 11px;text-align:right;color:#777;">{fmt_vol(r['avg_vol_20d'])}</td>
          <td style="padding:8px 11px;text-align:right;color:#555;">{fmt_vol(r['volume'])}</td>
        </tr>"""
    return html


def rows_5d(lst, mode="pct", bullish=True):
    """Table rows for 5-day lists. mode = 'pct' or 'pkr'."""
    html = ""
    for i, r in enumerate(lst):
        bg  = "#f9fafb" if i % 2 == 0 else "#fff"
        chg_col = "#2e7d32" if bullish else "#c62828"
        if mode == "pkr":
            move_cell = f'<td style="padding:8px 11px;text-align:right;">{pkr_badge(r["pkr_5d"])}</td>'
            pct_cell  = f'<td style="padding:8px 11px;text-align:right;">{pct_badge(r["pct_5d"])}</td>'
        else:
            move_cell = f'<td style="padding:8px 11px;text-align:right;">{pct_badge(r["pct_5d"])}</td>'
            pct_cell  = f'<td style="padding:8px 11px;text-align:right;font-weight:bold;color:{chg_col};">{"+" if bullish else ""}PKR {r["pkr_5d"]:,.2f}</td>'
        html += f"""<tr style="background:{bg};">
          <td style="padding:8px 11px;font-weight:bold;color:#1a1a2e;">{r['symbol']}</td>
          <td style="padding:8px 11px;color:#666;font-size:11.5px;">{r['sector']}</td>
          <td style="padding:8px 11px;text-align:right;color:#555;">{r['price_5d_ago']:,.2f}</td>
          <td style="padding:8px 11px;text-align:right;font-weight:bold;">{r['price']:,.2f}</td>
          {move_cell}
          {pct_cell}
          <td style="padding:8px 11px;text-align:right;color:#777;">{fmt_vol(r['avg_vol_5d'])}</td>
          <td style="padding:8px 11px;text-align:right;color:#555;">{fmt_vol(r['volume'])}</td>
        </tr>"""
    return html


# ── Email Builder ──────────────────────────────────────────────────────────────
def build_email(l1, l2, l3, l4, l5, l6, scan_date, total_scanned):

    subject = (
        f"PSX Trend Scanner {scan_date} — "
        f"🚀{len(l1)} | 💥{len(l2)} | 📈{len(l3)} | 📉{len(l4)} | "
        f"💰{len(l5)} | 🔻{len(l6)}"
    )

    COLS_20D = ["Symbol","Sector","Price 20d Ago","Price Now","PKR Change","% Change","Avg Vol (20d)","Today's Vol"]
    COLS_5D  = ["Symbol","Sector","Price 5d Ago", "Price Now","% Change",  "PKR Change","Avg Vol (5d)", "Today's Vol"]
    COLS_PKR = ["Symbol","Sector","Price 5d Ago", "Price Now","PKR Change","% Change",  "Avg Vol (5d)", "Today's Vol"]

    def section(emoji, title, subtitle, colour, lst, col_headers, row_fn, empty_text):
        hdr = sec_hdr(emoji, title, f"{subtitle} &nbsp;|&nbsp; {len(lst)} stock(s) found", colour)
        if lst:
            return hdr + make_table(col_headers, row_fn(lst))
        return hdr + empty_note(empty_text)

    # ── Build each section ────────────────────────────────────────────────────
    sec1 = section(
        "🚀", "Up 50%+ in Last 20 Days", "Bullish — Strong Uptrend | Avg Vol ≥ 100K",
        "#1b5e20", l1, COLS_20D,
        lambda lst: rows_20d(lst, bullish=True),
        "No stocks up 50%+ over 20 days with sufficient volume today."
    )
    sec2 = section(
        "💥", "Down 50%+ in Last 20 Days", "Bearish — Strong Downtrend | Avg Vol ≥ 100K",
        "#b71c1c", l2, COLS_20D,
        lambda lst: rows_20d(lst, bullish=False),
        "No stocks down 50%+ over 20 days with sufficient volume today."
    )
    sec3 = section(
        "📈", "Up 20%+ in Last 5 Days", "Bullish — Short-Term Momentum | Avg Vol ≥ 100K",
        "#2e7d32", l3, COLS_5D,
        lambda lst: rows_5d(lst, mode="pct", bullish=True),
        "No stocks up 20%+ over 5 days with sufficient volume today."
    )
    sec4 = section(
        "📉", "Down 20%+ in Last 5 Days", "Bearish — Short-Term Breakdown | Avg Vol ≥ 100K",
        "#c62828", l4, COLS_5D,
        lambda lst: rows_5d(lst, mode="pct", bullish=False),
        "No stocks down 20%+ over 5 days with sufficient volume today."
    )
    sec5 = section(
        "💰", "Up PKR 20+ in Last 5 Days", "Bullish — Absolute Price Gain | Avg Vol ≥ 100K",
        "#1565c0", l5, COLS_PKR,
        lambda lst: rows_5d(lst, mode="pkr", bullish=True),
        "No stocks up PKR 20+ over 5 days with sufficient volume today."
    )
    sec6 = section(
        "🔻", "Down PKR 20+ in Last 5 Days", "Bearish — Absolute Price Loss | Avg Vol ≥ 100K",
        "#6a1b9a", l6, COLS_PKR,
        lambda lst: rows_5d(lst, mode="pkr", bullish=False),
        "No stocks down PKR 20+ over 5 days with sufficient volume today."
    )

    html = f"""<html>
<body style="font-family:Arial,sans-serif;max-width:960px;margin:auto;
             padding:20px;background:#f0f0f0;">

  <!-- Header -->
  <div style="background:#0d1b2a;padding:22px 26px;border-radius:10px 10px 0 0;">
    <h1 style="color:#fff;margin:0;font-size:22px;">📊 PSX Trend Scanner</h1>
    <p style="color:#aaa;margin:6px 0 0;font-size:13px;">
      {scan_date} &nbsp;|&nbsp; Multi-Day Trend Report &nbsp;|&nbsp;
      Pakistan Stock Exchange &nbsp;|&nbsp; {total_scanned} stocks scanned
    </p>
  </div>

  <!-- Quick count bar -->
  <div style="background:#fff;border:1px solid #ddd;border-top:none;
              padding:16px 26px;display:flex;gap:20px;flex-wrap:wrap;align-items:center;">

    <div style="text-align:center;min-width:80px;">
      <div style="font-size:24px;font-weight:bold;color:#1b5e20;">{len(l1)}</div>
      <div style="font-size:10px;color:#999;line-height:1.4;">🚀 Up 50%<br>20 days</div>
    </div>
    <div style="text-align:center;min-width:80px;">
      <div style="font-size:24px;font-weight:bold;color:#b71c1c;">{len(l2)}</div>
      <div style="font-size:10px;color:#999;line-height:1.4;">💥 Down 50%<br>20 days</div>
    </div>
    <div style="background:#e0e0e0;width:1px;height:50px;"></div>
    <div style="text-align:center;min-width:80px;">
      <div style="font-size:24px;font-weight:bold;color:#2e7d32;">{len(l3)}</div>
      <div style="font-size:10px;color:#999;line-height:1.4;">📈 Up 20%<br>5 days</div>
    </div>
    <div style="text-align:center;min-width:80px;">
      <div style="font-size:24px;font-weight:bold;color:#c62828;">{len(l4)}</div>
      <div style="font-size:10px;color:#999;line-height:1.4;">📉 Down 20%<br>5 days</div>
    </div>
    <div style="background:#e0e0e0;width:1px;height:50px;"></div>
    <div style="text-align:center;min-width:80px;">
      <div style="font-size:24px;font-weight:bold;color:#1565c0;">{len(l5)}</div>
      <div style="font-size:10px;color:#999;line-height:1.4;">💰 +PKR 20<br>5 days</div>
    </div>
    <div style="text-align:center;min-width:80px;">
      <div style="font-size:24px;font-weight:bold;color:#6a1b9a;">{len(l6)}</div>
      <div style="font-size:10px;color:#999;line-height:1.4;">🔻 -PKR 20<br>5 days</div>
    </div>

  </div>

  <!-- Legend -->
  <div style="background:#fffde7;border:1px solid #ddd;border-top:none;
              padding:10px 26px;font-size:11.5px;color:#666;">
    <strong>How to read:</strong>
    All lists require average daily volume ≥ 100,000 over the lookback period.
    "Price Ago" = closing price at the start of the lookback window.
    "Avg Vol" = average daily volume over the lookback period.
    "Today's Vol" = volume traded today.
  </div>

  <!-- All sections -->
  <div style="background:#fff;border:1px solid #ddd;border-top:none;
              padding:20px 26px 30px;border-radius:0 0 10px 10px;">
    {sec1}
    {sec2}
    {sec3}
    {sec4}
    {sec5}
    {sec6}
    <hr style="border:none;border-top:1px solid #eee;margin:30px 0 14px;">
    <p style="font-size:11px;color:#bbb;margin:0;line-height:1.6;">
      Data sourced from
      <a href="https://psxterminal.com" style="color:#bbb;">psxterminal.com</a>.
      PSX closes 3:30 PM PKT. This report runs at 3:35 PM PKT Mon–Fri.<br>
      Lookback uses trading days only (excludes weekends and holidays).<br>
      For informational purposes only — not financial advice.
    </p>
  </div>

</body></html>"""

    return subject, html


# ── Email Sender ───────────────────────────────────────────────────────────────
def send_email(subject, html_body):
    print(f"\n[EMAIL] {SENDER_EMAIL} → {RECIPIENT_EMAIL}")
    if not SENDER_EMAIL:
        print("[EMAIL] ❌ SENDER_EMAIL missing"); sys.exit(1)
    if not SENDER_PASSWORD:
        print("[EMAIL] ❌ SENDER_PASSWORD missing"); sys.exit(1)

    msg            = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SENDER_EMAIL
    msg["To"]      = RECIPIENT_EMAIL
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, msg.as_string())
            print(f"[EMAIL] ✅ Sent to {RECIPIENT_EMAIL}")
    except smtplib.SMTPAuthenticationError as e:
        print(f"[EMAIL] ❌ AUTH FAILED: {e}"); sys.exit(1)
    except Exception as e:
        print(f"[EMAIL] ❌ ERROR: {e}"); traceback.print_exc(); sys.exit(1)


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    scan_date = datetime.now().strftime("%B %d, %Y")
    print(f"\n{'='*62}")
    print(f"  PSX Trend Scanner v1  —  {scan_date}")
    print(f"{'='*62}\n")

    symbols    = get_symbols()
    if not symbols:
        print("[MAIN] ❌ Could not fetch symbols. Aborting.")
        sys.exit(1)

    sector_map = get_sector_map(symbols)
    results    = fetch_trend_data(symbols, sector_map)

    if not results:
        print("[MAIN] ❌ No trend data. Aborting.")
        sys.exit(1)

    l1, l2, l3, l4, l5, l6 = build_trend_lists(results)

    print(f"\n{'─'*50}")
    print(f"  Stocks analysed        : {len(results)}")
    print(f"  🚀 Up 50%+ / 20d       : {len(l1)}")
    print(f"  💥 Down 50%+ / 20d     : {len(l2)}")
    print(f"  📈 Up 20%+ / 5d        : {len(l3)}")
    print(f"  📉 Down 20%+ / 5d      : {len(l4)}")
    print(f"  💰 Up PKR 20+ / 5d     : {len(l5)}")
    print(f"  🔻 Down PKR 20+ / 5d   : {len(l6)}")
    print(f"{'─'*50}")

    subject, html = build_email(l1, l2, l3, l4, l5, l6, scan_date, len(results))
    print(f"\n[EMAIL] Subject: {subject}")
    send_email(subject, html)
    print(f"\n{'='*62}\n")


if __name__ == "__main__":
    main()
