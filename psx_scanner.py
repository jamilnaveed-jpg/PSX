"""
PSX Daily Stock Scanner — v9
=====================================================================
Data source: psxterminal.com (confirmed working)

CHANGES FROM v8:
  - Full per-sector breakdown replacing simple top-5 table
  - Every sector card shows: total volume, total value, avg change%
    (volume-weighted), gainers/losers/unchanged count, mini breadth
    bar, and top 3 stocks by volume within the sector
  - Sectors sorted by total volume descending

LIST 1 — Momentum Gainers   : change >= +4%   AND volume >= 100,000
LIST 2 — High-Volume Movers : volume >= 9,000,000  (any direction)
LIST 3 — Tight Range Watch  : |change%| <= 0.40%
LIST 4 — Top Losers         : change <= -4%   AND volume >= 100,000
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

GAINER_PCT      = 4.0        # List 1: minimum % gain
LOSER_PCT       = -4.0       # List 4: maximum % loss
MIN_VOLUME_L1   = 100_000    # List 1 & 4: minimum volume
MIN_VOLUME_L2   = 9_000_000  # List 2: high-volume threshold (no % filter)
TIGHT_RANGE_PCT = 0.40       # List 3: max absolute % change


# ── Step 1: Get all symbols ────────────────────────────────────────────────────
def get_symbols():
    url  = f"{BASE_URL}/api/symbols"
    resp = requests.get(url, headers=HEADERS, timeout=20)
    data = resp.json()
    raw  = data.get("data", data) if isinstance(data, dict) else data
    symbols = [str(s).strip() for s in raw if s]
    print(f"[SYMBOLS] {len(symbols)} symbols fetched")
    return symbols


# ── Step 2: Fetch sector map for all symbols ───────────────────────────────────
def get_sector_map(symbols):
    """
    Fetches /api/companies/{symbol} for each symbol to get sector info.
    Returns dict: { "OGDC": "Oil & Gas Exploration", ... }
    Runs in one pass alongside tick fetching to avoid double looping.
    """
    sector_map = {}
    errors = 0
    print(f"[SECTORS] Fetching company/sector data for {len(symbols)} symbols...")

    for i, sym in enumerate(symbols):
        try:
            url  = f"{BASE_URL}/api/companies/{sym}"
            resp = requests.get(url, headers=HEADERS, timeout=8)
            if resp.status_code == 200:
                data = resp.json()
                info = data.get("data", {}) if isinstance(data, dict) else {}
                if isinstance(info, dict):
                    sector = (
                        info.get("sector")
                        or info.get("sectorName")
                        or info.get("industry")
                        or info.get("Sector")
                        or "—"
                    )
                    sector_map[sym] = str(sector).strip() or "—"
                else:
                    sector_map[sym] = "—"
            else:
                sector_map[sym] = "—"
        except Exception:
            sector_map[sym] = "—"
            errors += 1

        if (i + 1) % 100 == 0:
            print(f"[SECTORS] {i+1}/{len(symbols)} done, {errors} errors")
        if (i + 1) % 50 == 0:
            time.sleep(0.2)

    filled = sum(1 for v in sector_map.values() if v != "—")
    print(f"[SECTORS] Complete — {filled}/{len(symbols)} sectors found")
    return sector_map


# ── Step 3: Fetch tick for one symbol ─────────────────────────────────────────
def get_tick(symbol):
    url  = f"{BASE_URL}/api/ticks/REG/{symbol}"
    resp = requests.get(url, headers=HEADERS, timeout=10)
    if resp.status_code != 200:
        return None
    data = resp.json()
    tick = data.get("data", {}) if isinstance(data, dict) else {}
    if not tick or not isinstance(tick, dict):
        return None

    price      = float(tick.get("price",        0) or 0)
    change     = float(tick.get("change",        0) or 0)
    change_pct = float(tick.get("changePercent", 0) or 0) * 100  # decimal → %
    volume     = int(  tick.get("volume",        0) or 0)
    high       = float(tick.get("high",          0) or 0)
    low        = float(tick.get("low",           0) or 0)
    trades     = int(  tick.get("trades",        0) or 0)
    prev_close = round(price - change, 2) if price else 0

    return {
        "symbol":     symbol,
        "sector":     "—",          # filled in later from sector_map
        "price":      price,
        "prev_close": prev_close,
        "change":     change,
        "change_pct": round(change_pct, 2),
        "volume":     volume,
        "high":       high,
        "low":        low,
        "trades":     trades,
    }


# ── Step 4: Fetch everything ───────────────────────────────────────────────────
def fetch_all_stocks():
    symbols = get_symbols()
    if not symbols:
        return [], {}, "failed to get symbols"

    # Fetch sectors first (parallel loop)
    sector_map = get_sector_map(symbols)

    # Fetch ticks
    stocks = []
    errors = 0
    total  = len(symbols)
    print(f"\n[TICKS] Fetching {total} ticks...")

    for i, sym in enumerate(symbols):
        try:
            tick = get_tick(sym)
            if tick and tick["price"] > 0:
                tick["sector"] = sector_map.get(sym, "—")
                stocks.append(tick)
        except Exception:
            errors += 1

        if (i + 1) % 100 == 0:
            print(f"[TICKS] {i+1}/{total} — {len(stocks)} valid, {errors} errors")
        if (i + 1) % 50 == 0:
            time.sleep(0.3)

    print(f"[TICKS] Complete — {len(stocks)} stocks, {errors} errors")

    # Sample log
    sample = sorted(stocks, key=lambda x: abs(x["change_pct"]), reverse=True)[:5]
    for s in sample:
        d = "▲" if s["change_pct"] > 0 else "▼"
        print(f"  {s['symbol']:10s} {d}{abs(s['change_pct']):.2f}%  {s['sector']}")

    return stocks, sector_map, "psxterminal.com"


# ── Market Summary ─────────────────────────────────────────────────────────────
def market_summary(stocks):
    """
    Compute overall market stats + full per-sector breakdown.
    Each sector entry contains:
      total_vol, total_val, gainers, losers, unchanged,
      avg_change_pct (volume-weighted), top3 stocks by volume
    """
    gainers   = sum(1 for s in stocks if s["change_pct"] > 0)
    losers    = sum(1 for s in stocks if s["change_pct"] < 0)
    unchanged = sum(1 for s in stocks if s["change_pct"] == 0)
    total_vol = sum(s["volume"] for s in stocks)
    total_val = sum(s["price"] * s["volume"] for s in stocks)

    # Build per-sector dict
    sectors = {}
    for s in stocks:
        sec = s["sector"] if s["sector"] not in ("—", "", None) else "Unknown"
        if sec not in sectors:
            sectors[sec] = {
                "stocks":    [],
                "total_vol": 0,
                "total_val": 0,
                "gainers":   0,
                "losers":    0,
                "unchanged": 0,
                "wtd_pct":   0.0,   # volume-weighted avg change%
            }
        d = sectors[sec]
        d["stocks"].append(s)
        d["total_vol"] += s["volume"]
        d["total_val"] += s["price"] * s["volume"]
        d["wtd_pct"]   += s["change_pct"] * s["volume"]  # accumulate; divide later
        if s["change_pct"] > 0:
            d["gainers"]   += 1
        elif s["change_pct"] < 0:
            d["losers"]    += 1
        else:
            d["unchanged"] += 1

    # Finalise each sector
    sector_list = []
    for name, d in sectors.items():
        avg_pct = round(d["wtd_pct"] / d["total_vol"], 2) if d["total_vol"] > 0 else 0.0
        # Top 3 stocks by volume within sector
        top3 = sorted(d["stocks"], key=lambda x: x["volume"], reverse=True)[:3]
        sector_list.append({
            "name":      name,
            "total_vol": d["total_vol"],
            "total_val": d["total_val"],
            "gainers":   d["gainers"],
            "losers":    d["losers"],
            "unchanged": d["unchanged"],
            "count":     len(d["stocks"]),
            "avg_pct":   avg_pct,
            "top3":      top3,
        })

    # Sort sectors by total volume descending
    sector_list.sort(key=lambda x: x["total_vol"], reverse=True)

    return {
        "gainers":     gainers,
        "losers":      losers,
        "unchanged":   unchanged,
        "total":       len(stocks),
        "total_vol":   total_vol,
        "total_val":   total_val,
        "sectors":     sector_list,
    }


# ── Filters ────────────────────────────────────────────────────────────────────
def build_lists(stocks):
    list1, list2, list3, list4 = [], [], [], []
    for s in stocks:
        pct = s["change_pct"]
        vol = s["volume"]
        # List 1: Momentum Gainers
        if pct >= GAINER_PCT and vol >= MIN_VOLUME_L1:
            list1.append(s)
        # List 2: High-Volume (any direction, just 9M+)
        if vol >= MIN_VOLUME_L2:
            list2.append(s)
        # List 3: Tight Range
        if abs(pct) <= TIGHT_RANGE_PCT:
            list3.append(s)
        # List 4: Top Losers
        if pct <= LOSER_PCT and vol >= MIN_VOLUME_L1:
            list4.append(s)

    list1.sort(key=lambda x: x["change_pct"], reverse=True)   # biggest gain first
    list2.sort(key=lambda x: x["volume"],     reverse=True)   # biggest volume first
    list3.sort(key=lambda x: abs(x["change_pct"]))            # tightest range first
    list4.sort(key=lambda x: x["change_pct"])                 # biggest loss first
    return list1, list2, list3, list4


# ── HTML Helpers ───────────────────────────────────────────────────────────────
def fmt_vol(v):
    if not isinstance(v, (int, float)):
        return str(v)
    if v >= 1_000_000:
        return f"{v/1_000_000:.2f}M"
    if v >= 1_000:
        return f"{v/1_000:.1f}K"
    return f"{v:,}"


def fmt_val(v):
    if v >= 1_000_000_000:
        return f"PKR {v/1_000_000_000:.2f}B"
    if v >= 1_000_000:
        return f"PKR {v/1_000_000:.1f}M"
    return f"PKR {v:,.0f}"


def pct_badge(pct):
    if pct > 0:
        bg, fg, sign = "#e8f5e9", "#2e7d32", "+"
    elif pct < 0:
        bg, fg, sign = "#fdecea", "#c62828", ""
    else:
        bg, fg, sign = "#f5f5f5", "#666", ""
    return (
        f'<span style="background:{bg};color:{fg};padding:3px 9px;'
        f'border-radius:10px;font-weight:bold;font-size:12px;">'
        f'{sign}{pct:.2f}%</span>'
    )


def make_table(rows_html, col_headers):
    ths = ""
    for i, h in enumerate(col_headers):
        align = "left" if i <= 1 else "right"
        ths += f'<th style="padding:10px 12px;text-align:{align};font-weight:600;white-space:nowrap;">{h}</th>'
    return f"""
    <table style="width:100%;border-collapse:collapse;font-size:13px;
                  font-family:Arial,sans-serif;border:1px solid #e0e0e0;
                  border-radius:6px;overflow:hidden;margin-bottom:4px;">
      <thead><tr style="background:#1a1a2e;color:#fff;">{ths}</tr></thead>
      <tbody>{rows_html}</tbody>
    </table>"""


def sec_hdr(emoji, title, subtitle, colour):
    return f"""
    <div style="background:{colour};border-radius:8px 8px 0 0;
                padding:12px 18px;margin-top:30px;">
      <h3 style="margin:0;color:#fff;font-size:15px;">{emoji}&nbsp; {title}</h3>
      <p style="margin:3px 0 0;color:rgba(255,255,255,0.75);font-size:11.5px;">{subtitle}</p>
    </div>"""


def empty_msg(text):
    return f'<p style="color:#aaa;font-style:italic;padding:10px 0 22px;">{text}</p>'


def stock_rows(lst, show_direction=False, loss_mode=False):
    """Generate table rows for a stock list."""
    rows = ""
    for i, s in enumerate(lst):
        bg = "#f9fafb" if i % 2 == 0 else "#fff"
        chg_color = "#c62828" if loss_mode else "#2e7d32"
        chg_sign  = "" if loss_mode else "+"
        sector_cell = f'<td style="padding:8px 12px;color:#666;font-size:12px;">{s["sector"]}</td>'

        if show_direction:
            if s['change_pct'] > 0:
                d = '<span style="color:#2e7d32;font-weight:bold;">▲</span>'
            elif s['change_pct'] < 0:
                d = '<span style="color:#c62828;font-weight:bold;">▼</span>'
            else:
                d = '<span style="color:#aaa;">—</span>'
            rows += f"""<tr style="background:{bg};">
              <td style="padding:8px 12px;font-weight:bold;color:#1a1a2e;">{s['symbol']}</td>
              {sector_cell}
              <td style="padding:8px 12px;text-align:right;">{s['price']:,.2f}</td>
              <td style="padding:8px 12px;text-align:right;">{pct_badge(s['change_pct'])}</td>
              <td style="padding:8px 12px;text-align:center;">{d}</td>
              <td style="padding:8px 12px;text-align:right;color:#777;">{s['high']:,.2f} / {s['low']:,.2f}</td>
              <td style="padding:8px 12px;text-align:right;color:#555;">{fmt_vol(s['volume'])}</td>
            </tr>"""
        else:
            rows += f"""<tr style="background:{bg};">
              <td style="padding:8px 12px;font-weight:bold;color:#1a1a2e;">{s['symbol']}</td>
              {sector_cell}
              <td style="padding:8px 12px;text-align:right;">{s['price']:,.2f}</td>
              <td style="padding:8px 12px;text-align:right;font-weight:bold;color:{chg_color};">
                  {chg_sign}{s['change']:,.2f}</td>
              <td style="padding:8px 12px;text-align:right;">{pct_badge(s['change_pct'])}</td>
              <td style="padding:8px 12px;text-align:right;color:#777;">{s['high']:,.2f} / {s['low']:,.2f}</td>
              <td style="padding:8px 12px;text-align:right;color:#555;">{fmt_vol(s['volume'])}</td>
            </tr>"""
    return rows


# ── Email Builder ──────────────────────────────────────────────────────────────
def build_email(list1, list2, list3, list4, mkt, scan_date, source_name):

    subject = (
        f"PSX {scan_date} — "
        f"▲{len(list1)} Gainers | 🔥{len(list2)} HiVol | "
        f"🔔{len(list3)} Tight | ▼{len(list4)} Losers"
    )

    COLS_MAIN  = ["Symbol","Sector","Price (PKR)","Change","Change %","High / Low","Volume"]
    COLS_TIGHT = ["Symbol","Sector","Price (PKR)","Change %","Dir","High / Low","Volume"]
    COLS_VOL   = ["Symbol","Sector","Price (PKR)","Change","Change %","High / Low","Volume"]

    # ── Market Overview numbers ───────────────────────────────────────────────
    g_pct = round(mkt["gainers"]   / mkt["total"] * 100) if mkt["total"] else 0
    l_pct = round(mkt["losers"]    / mkt["total"] * 100) if mkt["total"] else 0
    u_pct = round(mkt["unchanged"] / mkt["total"] * 100) if mkt["total"] else 0

    breadth_bar = f"""
    <div style="margin:14px 0 8px;">
      <div style="font-size:11px;color:#888;margin-bottom:4px;font-weight:600;">
          Market Breadth</div>
      <div style="display:flex;height:14px;border-radius:7px;overflow:hidden;">
        <div style="width:{g_pct}%;background:#2e7d32;"></div>
        <div style="width:{u_pct}%;background:#bdbdbd;"></div>
        <div style="width:{l_pct}%;background:#c62828;"></div>
      </div>
      <div style="display:flex;gap:18px;margin-top:5px;font-size:11.5px;">
        <span style="color:#2e7d32;font-weight:bold;">▲ {mkt['gainers']} Gainers ({g_pct}%)</span>
        <span style="color:#888;">— {mkt['unchanged']} Unchanged</span>
        <span style="color:#c62828;font-weight:bold;">▼ {mkt['losers']} Losers ({l_pct}%)</span>
      </div>
    </div>"""

    overview_html = f"""
    <div style="background:#f8f9fa;border:1px solid #e0e0e0;border-radius:8px;
                padding:18px 22px;margin-top:20px;">
      <h3 style="margin:0 0 16px;font-size:16px;color:#1a1a2e;">📊 Market Overview</h3>
      <div style="display:flex;gap:28px;flex-wrap:wrap;">
        <div style="text-align:center;min-width:80px;">
          <div style="font-size:26px;font-weight:bold;color:#1a1a2e;">{mkt['total']}</div>
          <div style="font-size:11px;color:#888;margin-top:2px;">Stocks</div>
        </div>
        <div style="text-align:center;min-width:80px;">
          <div style="font-size:26px;font-weight:bold;color:#2e7d32;">{mkt['gainers']}</div>
          <div style="font-size:11px;color:#888;margin-top:2px;">Gainers</div>
        </div>
        <div style="text-align:center;min-width:80px;">
          <div style="font-size:26px;font-weight:bold;color:#c62828;">{mkt['losers']}</div>
          <div style="font-size:11px;color:#888;margin-top:2px;">Losers</div>
        </div>
        <div style="text-align:center;min-width:80px;">
          <div style="font-size:26px;font-weight:bold;color:#888;">{mkt['unchanged']}</div>
          <div style="font-size:11px;color:#888;margin-top:2px;">Unchanged</div>
        </div>
        <div style="text-align:center;min-width:100px;">
          <div style="font-size:20px;font-weight:bold;color:#1a1a2e;">{fmt_vol(mkt['total_vol'])}</div>
          <div style="font-size:11px;color:#888;margin-top:2px;">Total Volume</div>
        </div>
        <div style="text-align:center;min-width:100px;">
          <div style="font-size:20px;font-weight:bold;color:#1a1a2e;">{fmt_val(mkt['total_val'])}</div>
          <div style="font-size:11px;color:#888;margin-top:2px;">Total Value</div>
        </div>
      </div>
      {breadth_bar}
    </div>"""

    # ── Per-Sector Breakdown ──────────────────────────────────────────────────
    def sector_card(sec):
        """Build one sector card with stats + top 3 stocks."""
        sp = round(sec["gainers"] / sec["count"] * 100) if sec["count"] else 0
        lp = round(sec["losers"]  / sec["count"] * 100) if sec["count"] else 0
        up = 100 - sp - lp

        # Sector net sentiment colour
        if sec["avg_pct"] > 0:
            hdr_col, arrow = "#2e7d32", "▲"
        elif sec["avg_pct"] < 0:
            hdr_col, arrow = "#c62828", "▼"
        else:
            hdr_col, arrow = "#607d8b", "—"

        avg_badge = pct_badge(sec["avg_pct"])

        # Mini breadth bar for the sector
        mini_bar = f"""
        <div style="display:flex;height:6px;border-radius:3px;overflow:hidden;margin:6px 0 3px;">
          <div style="width:{sp}%;background:#2e7d32;"></div>
          <div style="width:{up}%;background:#bdbdbd;"></div>
          <div style="width:{lp}%;background:#c62828;"></div>
        </div>
        <div style="font-size:10px;color:#999;">
          ▲{sec['gainers']} &nbsp;—{sec['unchanged']} &nbsp;▼{sec['losers']}
          &nbsp;|&nbsp; {sec['count']} stocks
        </div>"""

        # Top 3 stocks rows
        top3_rows = ""
        for t in sec["top3"]:
            chg_col = "#2e7d32" if t["change_pct"] > 0 else ("#c62828" if t["change_pct"] < 0 else "#888")
            sign    = "+" if t["change_pct"] > 0 else ""
            top3_rows += f"""
            <tr style="border-top:1px solid #f0f0f0;">
              <td style="padding:5px 8px;font-weight:bold;font-size:12px;color:#1a1a2e;">
                  {t['symbol']}</td>
              <td style="padding:5px 8px;text-align:right;font-size:12px;color:#444;">
                  {t['price']:,.2f}</td>
              <td style="padding:5px 8px;text-align:right;font-size:12px;
                         font-weight:bold;color:{chg_col};">
                  {sign}{t['change_pct']:.2f}%</td>
              <td style="padding:5px 8px;text-align:right;font-size:11px;color:#777;">
                  {fmt_vol(t['volume'])}</td>
            </tr>"""

        return f"""
        <div style="background:#fff;border:1px solid #e0e0e0;border-radius:8px;
                    overflow:hidden;margin-bottom:14px;">
          <!-- Sector header -->
          <div style="background:{hdr_col};padding:9px 14px;display:flex;
                      justify-content:space-between;align-items:center;">
            <span style="color:#fff;font-weight:bold;font-size:13px;">{sec['name']}</span>
            <div style="display:flex;gap:12px;align-items:center;">
              <span style="color:rgba(255,255,255,0.85);font-size:11.5px;">
                  Vol: {fmt_vol(sec['total_vol'])}</span>
              <span style="color:rgba(255,255,255,0.85);font-size:11.5px;">
                  Val: {fmt_val(sec['total_val'])}</span>
              <span style="background:rgba(255,255,255,0.2);color:#fff;padding:2px 8px;
                           border-radius:10px;font-size:11.5px;font-weight:bold;">
                  {arrow} Avg {sec['avg_pct']:+.2f}%</span>
            </div>
          </div>
          <!-- Breadth mini bar -->
          <div style="padding:8px 14px 4px;">{mini_bar}</div>
          <!-- Top 3 stocks -->
          <table style="width:100%;border-collapse:collapse;">
            <thead>
              <tr style="background:#f5f5f5;">
                <th style="padding:5px 8px;text-align:left;font-size:11px;
                           color:#888;font-weight:600;">Top by Vol</th>
                <th style="padding:5px 8px;text-align:right;font-size:11px;
                           color:#888;font-weight:600;">Price</th>
                <th style="padding:5px 8px;text-align:right;font-size:11px;
                           color:#888;font-weight:600;">Chg%</th>
                <th style="padding:5px 8px;text-align:right;font-size:11px;
                           color:#888;font-weight:600;">Volume</th>
              </tr>
            </thead>
            <tbody>{top3_rows}</tbody>
          </table>
        </div>"""

    # Build all sector cards (sorted by volume, already done in market_summary)
    all_sector_cards = "".join(sector_card(s) for s in mkt["sectors"])

    sector_breakdown_html = f"""
    <div style="margin-top:24px;">
      <h3 style="margin:0 0 14px;font-size:15px;color:#1a1a2e;
                 border-bottom:2px solid #e0e0e0;padding-bottom:8px;">
        🏭 Sector Breakdown
        <span style="font-size:12px;color:#999;font-weight:normal;">
            — {len(mkt['sectors'])} sectors, sorted by volume</span>
      </h3>
      {all_sector_cards}
    </div>"""

    market_summary_html = overview_html + sector_breakdown_html

    # ── Section 1: Momentum Gainers ──────────────────────────────────────────
    if list1:
        sec1 = (
            sec_hdr("📈","Momentum Gainers",
                f"Up 4%+ | Volume ≥ 100K | {len(list1)} stock(s) found today","#2e7d32")
            + make_table(stock_rows(list1), COLS_MAIN)
        )
    else:
        sec1 = (
            sec_hdr("📈","Momentum Gainers","Up 4%+ | Volume ≥ 100K","#2e7d32")
            + empty_msg("No stocks met the Momentum Gainer criteria today.")
        )

    # ── Section 2: High-Volume Movers (any direction, 9M+) ───────────────────
    if list2:
        sec2 = (
            sec_hdr("🔥","High-Volume Movers",
                f"Volume ≥ 9,000,000 — any direction | {len(list2)} stock(s) found today","#e65100")
            + make_table(stock_rows(list2), COLS_VOL)
        )
    else:
        sec2 = (
            sec_hdr("🔥","High-Volume Movers","Volume ≥ 9M — any direction","#e65100")
            + empty_msg("No stocks traded above 9M volume today.")
        )

    # ── Section 3: Tight Range Watch ─────────────────────────────────────────
    if list3:
        sec3 = (
            sec_hdr("🔔","Tight Range Watch",
                f"|Change| ≤ 0.40% (up or down) | {len(list3)} stock(s) consolidating today","#5c35a0")
            + make_table(stock_rows(list3, show_direction=True), COLS_TIGHT)
        )
    else:
        sec3 = (
            sec_hdr("🔔","Tight Range Watch","|Change| ≤ 0.40%","#5c35a0")
            + empty_msg("No tight-range stocks found today.")
        )

    # ── Section 4: Top Losers ─────────────────────────────────────────────────
    if list4:
        sec4 = (
            sec_hdr("📉","Top Losers",
                f"Down 4%+ | Volume ≥ 100K | {len(list4)} stock(s) found today","#c62828")
            + make_table(stock_rows(list4, loss_mode=True), COLS_MAIN)
        )
    else:
        sec4 = (
            sec_hdr("📉","Top Losers","Down 4%+ | Volume ≥ 100K","#c62828")
            + empty_msg("No stocks down 4%+ with significant volume today.")
        )

    # ── Full Email ────────────────────────────────────────────────────────────
    html = f"""<html>
<body style="font-family:Arial,sans-serif;max-width:960px;margin:auto;
             padding:20px;background:#f0f0f0;">

  <!-- Header -->
  <div style="background:#1a1a2e;padding:22px 26px;border-radius:10px 10px 0 0;">
    <h1 style="color:#fff;margin:0;font-size:22px;">📊 PSX Daily Stock Scanner</h1>
    <p style="color:#aaa;margin:6px 0 0;font-size:13px;">
      {scan_date} &nbsp;|&nbsp; End-of-Day Report &nbsp;|&nbsp; Pakistan Stock Exchange
    </p>
  </div>

  <!-- Quick count bar -->
  <div style="background:#fff;border:1px solid #ddd;border-top:none;
              padding:16px 26px;display:flex;gap:30px;flex-wrap:wrap;align-items:center;">
    <div style="text-align:center;min-width:90px;">
      <div style="font-size:28px;font-weight:bold;color:#2e7d32;">{len(list1)}</div>
      <div style="font-size:10px;color:#999;line-height:1.4;">Gainers<br>4%+ / ≥100K</div>
    </div>
    <div style="text-align:center;min-width:90px;">
      <div style="font-size:28px;font-weight:bold;color:#e65100;">{len(list2)}</div>
      <div style="font-size:10px;color:#999;line-height:1.4;">High Vol<br>≥9M any dir</div>
    </div>
    <div style="text-align:center;min-width:90px;">
      <div style="font-size:28px;font-weight:bold;color:#5c35a0;">{len(list3)}</div>
      <div style="font-size:10px;color:#999;line-height:1.4;">Tight Range<br>≤0.40%</div>
    </div>
    <div style="text-align:center;min-width:90px;">
      <div style="font-size:28px;font-weight:bold;color:#c62828;">{len(list4)}</div>
      <div style="font-size:10px;color:#999;line-height:1.4;">Losers<br>-4%+ / ≥100K</div>
    </div>
  </div>

  <!-- Main content -->
  <div style="background:#fff;border:1px solid #ddd;border-top:none;
              padding:20px 26px 30px;border-radius:0 0 10px 10px;">

    {market_summary_html}

    {sec1}
    {sec2}
    {sec3}
    {sec4}

    <hr style="border:none;border-top:1px solid #eee;margin:28px 0 14px;">
    <p style="font-size:11px;color:#bbb;margin:0;line-height:1.6;">
      Data sourced from
      <a href="https://psxterminal.com" style="color:#bbb;">psxterminal.com</a>.
      PSX closes 3:30 PM PKT. This report runs at 3:35 PM PKT Mon–Fri.<br>
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
    print(f"  PSX Daily Scanner v9  —  {scan_date}")
    print(f"{'='*62}\n")

    all_stocks, sector_map, source_name = fetch_all_stocks()

    if not all_stocks:
        print("\n[MAIN] ⚠️ No data — aborting")
        sys.exit(1)

    mkt                     = market_summary(all_stocks)
    list1, list2, list3, list4 = build_lists(all_stocks)

    print(f"\n{'─'*50}")
    print(f"  Market : {mkt['gainers']} up / {mkt['losers']} down / {mkt['unchanged']} flat")
    print(f"  Vol    : {fmt_vol(mkt['total_vol'])}   Value: {fmt_val(mkt['total_val'])}")
    print(f"  List 1 Gainers   (+4%/100K) : {len(list1)}")
    print(f"  List 2 HiVol     (9M+ any)  : {len(list2)}")
    print(f"  List 3 TightRange(≤0.40%)   : {len(list3)}")
    print(f"  List 4 Losers    (-4%/100K) : {len(list4)}")
    print(f"{'─'*50}")

    subject, html = build_email(
        list1, list2, list3, list4, mkt, scan_date, source_name
    )
    print(f"\n[EMAIL] Subject: {subject}")
    send_email(subject, html)
    print(f"\n{'='*62}\n")


if __name__ == "__main__":
    main()
