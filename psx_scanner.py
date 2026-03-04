"""
PSX ENDPOINT DIAGNOSTIC TOOL
=====================================================================
Run this FIRST on GitHub Actions to see exactly what each URL returns.
It prints full response details so we can identify the correct endpoint.
Upload this as psx_scanner.py, run it once, share the log, then we fix.
=====================================================================
"""

import requests
import json
import sys

URLS_TO_TEST = [
    # psxterminal.com - open source PSX API
    ("psxterminal /api/stats/REG",      "https://psxterminal.com/api/stats/REG"),
    ("psxterminal /api/symbols",         "https://psxterminal.com/api/symbols"),
    ("psxterminal /api/ticks/REG/OGDC", "https://psxterminal.com/api/ticks/REG/OGDC"),

    # dps.psx.com.pk - official PSX data portal
    ("dps /market-watch",               "https://dps.psx.com.pk/market-watch"),
    ("dps /chain?i=KSE100",            "https://dps.psx.com.pk/chain?i=KSE100"),
    ("dps /data/index-members/KSE100", "https://dps.psx.com.pk/data/index-members/KSE100"),
    ("dps /data/equities",             "https://dps.psx.com.pk/data/equities"),
    ("dps /data/stats/equities",       "https://dps.psx.com.pk/data/stats/equities"),
    ("dps /data/market",               "https://dps.psx.com.pk/data/market"),

    # psx.com.pk - main PSX website
    ("psx /download?dir=equities",     "https://www.psx.com.pk/download?dir=equities"),
]

HEADER_SETS = [
    # Try 1: Browser-like with psxterminal origin
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/html, */*",
        "Origin": "https://psxterminal.com",
        "Referer": "https://psxterminal.com/",
    },
    # Try 2: Browser-like with psx origin
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/html, */*",
        "Origin": "https://dps.psx.com.pk",
        "Referer": "https://dps.psx.com.pk/",
    },
]


def test_url(label, url, headers):
    print(f"\n  URL   : {url}")
    try:
        resp = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
        print(f"  Status: {resp.status_code}")
        print(f"  Type  : {resp.headers.get('Content-Type', 'unknown')}")
        print(f"  Size  : {len(resp.content)} bytes")

        if resp.status_code == 200:
            content = resp.text[:800].strip()
            # Try to parse as JSON and show structure
            try:
                data = resp.json()
                if isinstance(data, list):
                    print(f"  JSON  : LIST with {len(data)} items")
                    if len(data) > 0 and isinstance(data[0], dict):
                        print(f"  Keys  : {list(data[0].keys())}")
                        print(f"  Row 0 : {json.dumps(data[0], default=str)[:300]}")
                elif isinstance(data, dict):
                    print(f"  JSON  : DICT with keys: {list(data.keys())}")
                    # Show first nested list if any
                    for k, v in data.items():
                        if isinstance(v, list) and len(v) > 0:
                            print(f"  data['{k}'] is a list of {len(v)} items")
                            if isinstance(v[0], dict):
                                print(f"  Keys  : {list(v[0].keys())}")
                                print(f"  Row 0 : {json.dumps(v[0], default=str)[:300]}")
                            break
                print(f"  ✅ GOOD — JSON data found!")
                return True
            except Exception:
                print(f"  Not JSON. First 400 chars:")
                print(f"  {content[:400]}")
        elif resp.status_code in [301, 302, 307, 308]:
            print(f"  Redirect → {resp.headers.get('Location', '?')}")
        elif resp.status_code == 403:
            print(f"  ❌ 403 FORBIDDEN — blocked by server")
        elif resp.status_code == 404:
            print(f"  ❌ 404 NOT FOUND — endpoint does not exist")
        else:
            print(f"  ❌ Error response: {resp.text[:200]}")

    except requests.exceptions.ConnectionError:
        print(f"  ❌ CONNECTION ERROR — cannot reach host")
    except requests.exceptions.Timeout:
        print(f"  ❌ TIMEOUT")
    except Exception as e:
        print(f"  ❌ EXCEPTION: {e}")

    return False


def main():
    print("=" * 65)
    print("  PSX ENDPOINT DIAGNOSTIC — Testing all known data sources")
    print("=" * 65)

    working = []

    for label, url in URLS_TO_TEST:
        print(f"\n{'─'*65}")
        print(f"TESTING: {label}")

        found = False
        for i, headers in enumerate(HEADER_SETS):
            print(f"\n  [Header set {i+1}]")
            result = test_url(label, url, headers)
            if result:
                working.append((label, url, i+1))
                found = True
                break  # No need to try other header sets if this worked

        if not found:
            print(f"\n  ⚠️  No header set worked for: {label}")

    print(f"\n{'='*65}")
    print("SUMMARY — Working endpoints:")
    if working:
        for label, url, hset in working:
            print(f"  ✅ {label} (header set {hset})")
            print(f"     {url}")
    else:
        print("  ❌ NO WORKING ENDPOINTS FOUND")
        print("\n  This means GitHub Actions IP is being blocked by PSX.")
        print("  Solution: Use a proxy or a different free data source.")

    print("=" * 65)


if __name__ == "__main__":
    main()
