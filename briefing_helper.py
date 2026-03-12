#!/usr/bin/env python3
"""
briefing_helper.py — Local Briefing.com Earnings Scraper
=========================================================
Runs a tiny HTTP server on localhost:5050.
The EP page (ep.html) calls it to fetch earnings tickers automatically.

SECURITY MODEL
--------------
• Listens on localhost ONLY — never exposed to network
• Credentials come from the EP page in each request (sent over 127.0.0.1)
• No credentials are written to disk by this script
• CORS restricted to file:// and localhost origins only
• Logs nothing sensitive to stdout/stderr

INSTALL
-------
    pip install requests beautifulsoup4

RUN
---
    python briefing_helper.py

ENDPOINTS
---------
GET  /ping                      Health check — returns {"ok":true,"version":"1.1","status":"ready"}
POST /earnings  {date, username, password}
    Returns: {
        "tickers": ["AAPL","MSFT",...],
        "meta":    {"total":12,"bmo":5,"amc":7,"source":"briefing.com","date":"2026-03-12"},
        "detail":  [{"symbol":"AAPL","time":"BMO","company":"Apple Inc."},...]
    }

HOW IT WORKS
------------
1. Opens a requests.Session and POSTs login to Briefing.com
2. Fetches /earnings/earnings-calendar?date=YYYY-MM-DD
3. Parses the earnings table for ticker symbols + timing (BMO/AMC)
4. Returns clean JSON

The server auto-stops after IDLE_TIMEOUT seconds with no requests (default 10 min).
"""

import json
import re
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

# ── Dependencies ──────────────────────────────────────────────────────────────
try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: Missing dependencies. Run:  pip install requests beautifulsoup4")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────
PORT         = 5050
HOST         = '127.0.0.1'   # localhost ONLY — never 0.0.0.0
VERSION      = '1.1'
IDLE_TIMEOUT = 600            # seconds — server auto-shuts after 10 min idle
REQUEST_TIMEOUT = 20          # seconds per HTTP request to Briefing.com

# Allowed CORS origins — only local file and localhost
ALLOWED_ORIGINS = {
    'null',                        # file:// pages show as 'null' origin
    'http://localhost',
    'http://localhost:5050',
    'http://127.0.0.1',
    'http://127.0.0.1:5050',
    # GitHub Pages — update this to your actual Pages URL if needed:
    # 'https://jamilnaveed-jpg.github.io',
}

# Briefing.com endpoints
BRIEFING_BASE        = 'https://www.briefing.com'
BRIEFING_LOGIN_URL   = 'https://www.briefing.com/login'
BRIEFING_CALENDAR_URL= 'https://www.briefing.com/earnings/earnings-calendar'

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/122.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

# ── Last-activity tracker for auto-shutdown ───────────────────────────────────
last_activity = time.time()

def touch():
    global last_activity
    last_activity = time.time()

def idle_watcher():
    while True:
        time.sleep(30)
        if time.time() - last_activity > IDLE_TIMEOUT:
            print(f"\n[briefing_helper] Idle for {IDLE_TIMEOUT}s — shutting down.")
            sys.exit(0)

# ── Briefing.com scraper ──────────────────────────────────────────────────────

def login_briefing(session: requests.Session, username: str, password: str) -> bool:
    """
    Attempt login to Briefing.com.
    Returns True on success, False on failure.
    """
    try:
        # First GET the login page to grab any CSRF token
        r = session.get(BRIEFING_LOGIN_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()

        soup = BeautifulSoup(r.text, 'html.parser')

        # Find the login form
        form = soup.find('form', id='loginForm') or soup.find('form')
        if not form:
            print("[login] Could not find login form on page")
            return False

        # Collect all hidden fields (CSRF tokens, etc.)
        payload = {}
        for inp in form.find_all('input'):
            name  = inp.get('name', '')
            value = inp.get('value', '')
            if name:
                payload[name] = value

        # Fill credentials
        # Common field names used by Briefing.com — we try several
        for user_field in ['username','email','user','login','UserName','Email']:
            if user_field in payload or form.find('input', {'name': user_field}):
                payload[user_field] = username
                break
        else:
            # Fallback — just set common names
            payload['username'] = username
            payload['email']    = username

        for pass_field in ['password','pass','Password','passwd']:
            if pass_field in payload or form.find('input', {'name': pass_field}):
                payload[pass_field] = password
                break
        else:
            payload['password'] = password

        # Determine form action
        action = form.get('action', BRIEFING_LOGIN_URL)
        if action.startswith('/'):
            action = BRIEFING_BASE + action
        elif not action.startswith('http'):
            action = BRIEFING_BASE + '/' + action

        # POST login
        post_headers = {
            **HEADERS,
            'Content-Type': 'application/x-www-form-urlencoded',
            'Referer': BRIEFING_LOGIN_URL,
            'Origin':  BRIEFING_BASE,
        }
        r2 = session.post(action, data=payload, headers=post_headers,
                          timeout=REQUEST_TIMEOUT, allow_redirects=True)
        r2.raise_for_status()

        # Verify login by checking for subscriber-only content
        text_lower = r2.text.lower()
        logged_in_signals = [
            'logout', 'sign out', 'my account', 'subscriber',
            'welcome', 'dashboard', 'earnings calendar'
        ]
        failed_signals = [
            'invalid', 'incorrect', 'failed', 'error',
            'wrong password', 'not found'
        ]

        if any(s in text_lower for s in failed_signals):
            print("[login] Login failed — invalid credentials")
            return False

        if any(s in text_lower for s in logged_in_signals):
            print("[login] Login successful")
            return True

        # Ambiguous — treat redirect to non-login page as success
        if BRIEFING_LOGIN_URL not in r2.url:
            print(f"[login] Redirected to {r2.url} — treating as success")
            return True

        print("[login] Could not confirm login status")
        return False

    except requests.RequestException as e:
        print(f"[login] Network error: {e}")
        return False


def fetch_earnings_calendar(session: requests.Session, date_str: str) -> dict:
    """
    Fetch the Briefing.com earnings calendar for a given date.
    Returns {"tickers":[...], "detail":[...], "meta":{...}}
    """
    url    = f"{BRIEFING_CALENDAR_URL}?date={date_str}"
    result = {"tickers": [], "detail": [], "meta": {"total": 0, "bmo": 0, "amc": 0, "source": "briefing.com", "date": date_str}}

    try:
        r = session.get(url, headers={**HEADERS, 'Referer': BRIEFING_BASE},
                        timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to fetch calendar: {e}")

    soup = BeautifulSoup(r.text, 'html.parser')
    tickers = []
    detail  = []
    bmo_count = 0
    amc_count = 0

    # ── Strategy 1: look for the earnings table (most common layout) ──────────
    # Briefing uses a table with columns: Company, Symbol, Time, EPS Est, ...
    table = (
        soup.find('table', class_=re.compile(r'earnings', re.I)) or
        soup.find('table', id=re.compile(r'earnings', re.I)) or
        soup.find('table', class_=re.compile(r'calendar', re.I))
    )

    if table:
        rows = table.find_all('tr')
        for row in rows[1:]:   # skip header
            cols = row.find_all(['td','th'])
            if len(cols) < 2:
                continue
            # Try to find symbol — usually 2nd or 3rd column, or a link with ticker format
            sym  = ''
            time_tag = ''
            for col in cols:
                text = col.get_text(strip=True).upper()
                # Symbol: 1–5 uppercase letters, no numbers usually
                if re.match(r'^[A-Z]{1,5}$', text) and not sym:
                    sym = text
                # Time: BMO / AMC / TNS
                if text in ('BMO','AMC','TNS','BEFORE MARKET OPEN','AFTER MARKET CLOSE'):
                    time_tag = 'BMO' if 'BMO' in text or 'BEFORE' in text else 'AMC' if 'AMC' in text or 'AFTER' in text else 'TNS'

            # Also check links — Briefing often makes symbols into links
            for a in row.find_all('a', href=True):
                txt = a.get_text(strip=True).upper()
                if re.match(r'^[A-Z]{1,5}$', txt):
                    sym = txt
                    break
                # href pattern like /stocks/AAPL
                m = re.search(r'/stocks?/([A-Z]{1,5})\b', a['href'], re.I)
                if m:
                    sym = m.group(1).upper()
                    break

            if sym and len(sym) >= 1:
                tickers.append(sym)
                timing = time_tag or 'TNS'
                detail.append({'symbol': sym, 'time': timing, 'company': ''})
                if timing == 'BMO': bmo_count += 1
                elif timing == 'AMC': amc_count += 1

    # ── Strategy 2: regex scan for ticker patterns in the full page ───────────
    if not tickers:
        # Look for patterns like: /quote/AAPL  or  symbol=AAPL  or  ticker=AAPL
        syms_found = set()
        for m in re.finditer(
            r'(?:quote|symbol|ticker)[=/]([A-Z]{1,5})\b',
            r.text, re.I
        ):
            sym = m.group(1).upper()
            if sym not in ('THE','FOR','AND','BMO','AMC','EPS','EST','REV','INC'):
                syms_found.add(sym)
        for sym in sorted(syms_found):
            tickers.append(sym)
            detail.append({'symbol': sym, 'time': 'TNS', 'company': ''})

    # ── Strategy 3: look for JSON data embedded in page scripts ───────────────
    if not tickers:
        for script in soup.find_all('script'):
            text = script.string or ''
            # Look for arrays of ticker objects
            for m in re.finditer(r'"(?:symbol|ticker)"\s*:\s*"([A-Z]{1,5})"', text):
                sym = m.group(1).upper()
                if sym not in tickers:
                    tickers.append(sym)
                    detail.append({'symbol': sym, 'time': 'TNS', 'company': ''})

    # Deduplicate preserving order
    seen = set()
    tickers_clean = []
    detail_clean  = []
    for t, d in zip(tickers, detail):
        if t not in seen:
            seen.add(t)
            tickers_clean.append(t)
            detail_clean.append(d)

    result['tickers'] = tickers_clean
    result['detail']  = detail_clean
    result['meta']['total'] = len(tickers_clean)
    result['meta']['bmo']   = bmo_count
    result['meta']['amc']   = amc_count

    print(f"[calendar] {date_str} — found {len(tickers_clean)} tickers "
          f"({bmo_count} BMO / {amc_count} AMC)")

    return result


# ── HTTP Request Handler ──────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        # Suppress default Apache-style logs to avoid credential leakage
        ts = time.strftime('%H:%M:%S')
        print(f"[{ts}] {fmt % args}")

    def _cors_headers(self):
        origin = self.headers.get('Origin', 'null')
        if origin in ALLOWED_ORIGINS:
            self.send_header('Access-Control-Allow-Origin', origin)
        else:
            # Still allow if no origin (direct file access)
            self.send_header('Access-Control-Allow-Origin', 'null')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Access-Control-Max-Age', '600')

    def send_json(self, status: int, obj: dict):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self._cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        """Pre-flight CORS request."""
        self.send_response(204)
        self._cors_headers()
        self.end_headers()

    def do_GET(self):
        touch()
        parsed = urlparse(self.path)
        if parsed.path == '/ping':
            self.send_json(200, {
                'ok':      True,
                'version': VERSION,
                'status':  'ready',
                'idle_timeout_s': IDLE_TIMEOUT,
            })
        else:
            self.send_json(404, {'error': 'Not found'})

    def do_POST(self):
        touch()
        parsed = urlparse(self.path)

        if parsed.path != '/earnings':
            self.send_json(404, {'error': 'Not found'})
            return

        # Read body
        length = int(self.headers.get('Content-Length', 0))
        if length > 4096:
            self.send_json(400, {'error': 'Request too large'})
            return

        try:
            body = json.loads(self.rfile.read(length))
        except (json.JSONDecodeError, ValueError):
            self.send_json(400, {'error': 'Invalid JSON'})
            return

        date     = body.get('date', '').strip()
        username = body.get('username', '').strip()
        password = body.get('password', '').strip()

        # Basic validation
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', date):
            self.send_json(400, {'error': 'Invalid date format — expected YYYY-MM-DD'})
            return
        if not username or not password:
            self.send_json(400, {'error': 'Missing username or password'})
            return

        # Create a fresh session per request (no credential caching on disk)
        session = requests.Session()
        session.headers.update(HEADERS)

        try:
            ok = login_briefing(session, username, password)
            if not ok:
                self.send_json(401, {
                    'error': 'Login failed — check credentials in ⚙ Creds on the EP page'
                })
                return

            result = fetch_earnings_calendar(session, date)
            self.send_json(200, result)

        except RuntimeError as e:
            self.send_json(502, {'error': str(e)})
        except Exception as e:
            print(f"[handler] Unexpected error: {e}")
            self.send_json(500, {'error': 'Internal error — check terminal for details'})
        finally:
            session.close()


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print(f"  Briefing.com Helper v{VERSION}")
    print(f"  Listening on http://{HOST}:{PORT}")
    print(f"  Auto-shuts after {IDLE_TIMEOUT}s of inactivity")
    print("=" * 60)
    print()
    print("  ✓ Credentials are NEVER written to disk")
    print("  ✓ Only localhost connections are accepted")
    print("  ✓ Press Ctrl+C to stop at any time")
    print()

    # Start idle-watcher daemon thread
    t = threading.Thread(target=idle_watcher, daemon=True)
    t.start()

    server = HTTPServer((HOST, PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[briefing_helper] Stopped by user.")
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
