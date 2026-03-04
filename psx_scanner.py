"""
PSX FIELD NAME DIAGNOSTIC — v7 prep
Fetches raw JSON from psxterminal for 5 symbols and prints EVERY field.
This tells us exactly what field names to use for price, change%, volume.
"""

import requests
import json

BASE_URL = "https://psxterminal.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Accept":     "application/json, */*",
    "Origin":     "https://psxterminal.com",
    "Referer":    "https://psxterminal.com/",
}

TEST_SYMBOLS = ["KEL", "BOP", "OGDC", "HBL", "LUCK"]


def main():
    print("=" * 65)
    print("  PSX FIELD NAME DIAGNOSTIC")
    print("=" * 65)

    # ── Test 1: /api/stats/REG — print full raw response ──────────────────
    print("\n\n>>> TEST 1: /api/stats/REG — full raw response")
    print("─" * 65)
    try:
        url  = f"{BASE_URL}/api/stats/REG"
        resp = requests.get(url, headers=HEADERS, timeout=20)
        print(f"Status: {resp.status_code}, Size: {len(resp.content)} bytes")
        data = resp.json()
        print(f"Full raw JSON:\n{json.dumps(data, indent=2, default=str)[:3000]}")
    except Exception as e:
        print(f"FAILED: {e}")

    # ── Test 2: /api/ticks/REG/{symbol} — print raw for 5 symbols ─────────
    print("\n\n>>> TEST 2: /api/ticks/REG/{symbol} — raw JSON for each symbol")
    print("─" * 65)
    for sym in TEST_SYMBOLS:
        try:
            url  = f"{BASE_URL}/api/ticks/REG/{sym}"
            resp = requests.get(url, headers=HEADERS, timeout=10)
            print(f"\n--- {sym} (status:{resp.status_code}, {len(resp.content)} bytes) ---")
            data = resp.json()
            print(json.dumps(data, indent=2, default=str)[:800])
        except Exception as e:
            print(f"  FAILED: {e}")

    # ── Test 3: /api/symbols — print first 3 items to see structure ────────
    print("\n\n>>> TEST 3: /api/symbols — first 5 items")
    print("─" * 65)
    try:
        url  = f"{BASE_URL}/api/symbols"
        resp = requests.get(url, headers=HEADERS, timeout=20)
        data = resp.json()
        raw  = data.get("data", data) if isinstance(data, dict) else data
        print(f"Total symbols: {len(raw)}")
        print(f"First 5:\n{json.dumps(raw[:5], indent=2, default=str)}")
    except Exception as e:
        print(f"FAILED: {e}")

    # ── Test 4: try alternate bulk endpoints ──────────────────────────────
    print("\n\n>>> TEST 4: Other possible bulk endpoints")
    print("─" * 65)
    extras = [
        "/api/market/REG",
        "/api/quotes/REG",
        "/api/all/REG",
        "/api/live/REG",
        "/api/data/REG",
        "/api/stocks",
        "/api/quotes",
        "/api/market",
    ]
    for path in extras:
        try:
            url  = f"{BASE_URL}{path}"
            resp = requests.get(url, headers=HEADERS, timeout=8)
            print(f"\n  {path} → status:{resp.status_code}, size:{len(resp.content)} bytes")
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    raw  = data if isinstance(data, list) else data.get("data", [])
                    if isinstance(raw, list) and len(raw) > 0:
                        print(f"  LIST of {len(raw)} items. Keys: {list(raw[0].keys()) if isinstance(raw[0], dict) else 'N/A'}")
                        print(f"  Sample: {json.dumps(raw[0], default=str)[:300]}")
                    else:
                        print(f"  JSON keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")
                        print(f"  Raw: {json.dumps(data, default=str)[:300]}")
                except Exception:
                    print(f"  Not JSON: {resp.text[:200]}")
        except Exception as e:
            print(f"  {path} → FAILED: {e}")

    print("\n" + "=" * 65)
    print("  DIAGNOSTIC COMPLETE — share this full log")
    print("=" * 65)


if __name__ == "__main__":
    main()
