# #!/usr/bin/env python3
“””
Broker Availability RECON — Step 1 (Diagnostic-only)

## PURPOSE

This script does NOT update the main otoQ / Drive365 sheets.
Its ONE job is to answer, for each of the 20 broker websites, the questions:

```
1. Does the site load at all? (final URL, HTTP status, timing)
2. Does it need JavaScript, or is the supplier list in the initial HTML?
3. Does CloudFlare / DataDome / a CAPTCHA block us?
4. If rendered: is the brand ("otoQ" or "Drive365") visible on the page
   once JS has run? In which DOM selector / region?
5. Does the search page expose a JSON XHR/fetch endpoint that returns
   the supplier list directly? (If yes, Step 2 can skip the browser
   entirely for that broker.)
6. Which date format / query parameters does the URL use?
```

All findings are written to the Google Sheet as a “Recon_Diagnostics” tab,
one row per (broker × search scenario). Step 2 will read this tab and build
the production scrapers mechanically from it.

Test airport: HERAKLION (HER) only — it is in both otoQ and Drive365 sheets,
so every broker gets probed with a representative query.

Search date window: next week’s Monday -> Friday (weekday-only as per spec).

## REQUIREMENTS

- playwright (with chromium installed: `playwright install chromium --with-deps`)
- gspread
- google-auth
- Environment variables:
  GOOGLE_SHEETS_CREDENTIALS  (service-account JSON as string)
  SPREADSHEET_ID             (target Google Sheet)

This script is read-only toward the broker websites and write-only to
a NEW tab named “Recon_Diagnostics” on the spreadsheet. It will never
touch the otoQ or Drive365 sheets.
“””

import os
import sys
import json
import time
import traceback
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

# ─── Third-party ──────────────────────────────────────────────────────

try:
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
except ImportError:
print(“FATAL: playwright not installed. Run: pip install playwright && playwright install chromium –with-deps”)
sys.exit(1)

try:
import gspread
from google.oauth2.service_account import Credentials
except ImportError:
print(“FATAL: gspread / google-auth not installed.”)
sys.exit(1)

# ─── Configuration ────────────────────────────────────────────────────

TEST_AIRPORT = {
“city”: “Heraklion”,
“iata”: “HER”,
“country”: “Greece”,
“query”: “Heraklion Airport”,
“query_url”: “Heraklion+Airport”,
}

# Next week’s Monday -> Friday (weekday range per spec)

def next_week_weekday_range():
today = datetime.now(timezone.utc).date()
# Monday of next week
days_until_next_monday = (7 - today.weekday()) % 7
if days_until_next_monday == 0:
days_until_next_monday = 7
monday = today + timedelta(days=days_until_next_monday)
friday = monday + timedelta(days=4)
return monday, friday

PU_DATE, DO_DATE = next_week_weekday_range()
PU_STR = PU_DATE.strftime(”%Y-%m-%d”)
DO_STR = DO_DATE.strftime(”%Y-%m-%d”)
PU_TIME = “10:00”
DO_TIME = “10:00”
DRIVER_AGE = 30

# Brand strings we hunt for in rendered pages.

# We check both — Heraklion has both otoQ (always) and Drive365 (confirmed).

BRANDS = [“otoQ”, “Drive365”]

# Broader variants in case of spelling / casing quirks

BRAND_VARIANTS = {
“otoQ”:    [“otoq”, “oto q”, “oto-q”, “OTOQ”],
“Drive365”:[“drive365”, “drive 365”, “drive-365”, “DRIVE365”, “Drive 365”],
}

# ─── Broker URL candidates for Heraklion ──────────────────────────────

# For each broker we provide 1-3 candidate URL patterns to probe.

# The recon tries each until one returns HTTP 200 with non-empty body.

# Patterns verified in prior research (conversations + web_search).

BROKERS = [
{
“name”: “Discovercars.com”,
“strategy_hint”: “A_supplier_page”,
“candidates”: [
# Strategy A — structured supplier page
f”https://www.discovercars.com/partners/otoq-5844”,
f”https://www.discovercars.com/partners/drive365-5844”,  # guess; may 404
# Strategy B fallback — search results
f”https://www.discovercars.com/search-results?”
f”puCity=5844&doCity=5844&puDate={PU_STR}&doDate={DO_STR}”
f”&puHour=10&puMinute=00&doHour=10&doMinute=00&driverAge={DRIVER_AGE}”,
],
},
{
“name”: “Qeeq.com”,
“strategy_hint”: “B_search_scan”,
“candidates”: [
f”https://www.qeeq.com/car-rental/search?iata=HER”
f”&pickup_datetime={PU_STR}T{PU_TIME}&dropoff_datetime={DO_STR}T{DO_TIME}&age={DRIVER_AGE}”,
f”https://www.qeeq.com/car/car-rental-pro/airport/greece-heraklion-her”,
],
},
{
“name”: “Orbitcarhire.com”,
“strategy_hint”: “B_search_scan”,
“candidates”: [
f”https://www.orbitcarhire.com/heraklion-airport”
f”?puDate={PU_STR}&doDate={DO_STR}&puTime={PU_TIME}&doTime={DO_TIME}&age={DRIVER_AGE}”,
],
},
{
“name”: “carrental.hotelbeds.com”,
“strategy_hint”: “B_search_scan”,
“candidates”: [
f”https://carrental.hotelbeds.com/search?”
f”location=Heraklion+Airport&pickupDate={PU_STR}T{PU_TIME}”
f”&dropoffDate={DO_STR}T{DO_TIME}&driverAge={DRIVER_AGE}”,
],
},
{
“name”: “Enjoytravel.com”,
“strategy_hint”: “B_search_scan”,
“candidates”: [
f”https://www.enjoytravel.com/en/car-rental/search?”
f”pickUp=HER&dropOff=HER&dateFrom={PU_STR}+{PU_TIME}”
f”&dateTo={DO_STR}+{DO_TIME}&driverAge={DRIVER_AGE}”,
],
},
{
“name”: “Aurumcars.de”,
“strategy_hint”: “B_search_scan”,
“candidates”: [
f”https://www.aurumcars.de/en/search?”
f”location=Heraklion+Airport&from={PU_STR}&to={DO_STR}”
f”&pickup_time={PU_TIME}&dropoff_time={DO_TIME}&driver_age={DRIVER_AGE}”,
],
},
{
“name”: “Aurum”,  # alias of Aurumcars.de per prior mapping
“strategy_hint”: “B_search_scan_alias”,
“candidates”: [
f”https://www.aurumcars.de/en/search?”
f”location=Heraklion+Airport&from={PU_STR}&to={DO_STR}”
f”&pickup_time={PU_TIME}&dropoff_time={DO_TIME}&driver_age={DRIVER_AGE}”,
],
},
{
“name”: “Carjet.com”,
“strategy_hint”: “B_search_scan_hash_routing”,
“candidates”: [
f”https://www.carjet.com/en/car-hire/her”,
f”https://www.carjet.com/search#puLoc=HER&doLoc=HER”
f”&puDate={PU_STR}&doDate={DO_STR}&puTime={PU_TIME}&doTime={DO_TIME}”,
],
},
{
“name”: “Rentcars.com/en”,
“strategy_hint”: “B_search_scan”,
“candidates”: [
f”https://www.rentcars.com/en/search/heraklion-airport?”
f”from={PU_STR}&to={DO_STR}&pickup={PU_TIME}&dropoff={DO_TIME}&age={DRIVER_AGE}”,
],
},
{
“name”: “CarFlexi.com”,
“strategy_hint”: “B_search_scan”,
“candidates”: [
f”https://www.carflexi.com/search?pickup=HER&dropoff=HER”
f”&from={PU_STR}T{PU_TIME}&to={DO_STR}T{DO_TIME}&age={DRIVER_AGE}”,
],
},
{
“name”: “Economybookings.com”,
“strategy_hint”: “A_supplier_page”,
“candidates”: [
f”https://www.economybookings.com/en/suppliers/otoq/her”,
f”https://www.economybookings.com/en/suppliers/drive365/her”,
f”https://www.economybookings.com/search?”
f”pickup=HER&dropoff=HER&from={PU_STR}&to={DO_STR}”,
],
},
{
“name”: “Priceline.com/rental-cars”,
“strategy_hint”: “B_search_scan_heavy_protection”,
“candidates”: [
f”https://www.priceline.com/rental-cars/HER”,
f”https://www.priceline.com/drive/search/retail/results?”
f”pickup-location=HER&return-location=HER”
f”&pickup-date={PU_STR}&return-date={DO_STR}”
f”&pickup-time={PU_TIME}&return-time={DO_TIME}”,
],
},
{
“name”: “Rentcarla.com”,
“strategy_hint”: “B_search_scan”,
“candidates”: [
f”https://www.rentcarla.com/search?”
f”location=Heraklion+Airport&from={PU_STR}T{PU_TIME}”
f”&to={DO_STR}T{DO_TIME}&age={DRIVER_AGE}”,
],
},
{
“name”: “Vipcars.com”,
“strategy_hint”: “B_search_scan”,
“candidates”: [
f”https://www.vipcars.com/car-rental/greece/heraklion/heraklion-airport”,
],
},
{
“name”: “Yolcu360”,
“strategy_hint”: “B_search_scan”,
“candidates”: [
f”https://www.yolcu360.com/en/car-rental?”
f”pickUp=Heraklion+Airport&dropOff=Heraklion+Airport”
f”&pickUpDate={PU_STR}&dropOffDate={DO_STR}”
f”&pickUpTime={PU_TIME}&dropOffTime={DO_TIME}”,
],
},
{
“name”: “Wisecars.com”,
“strategy_hint”: “B_search_scan”,
“candidates”: [
f”https://www.wisecars.com/en-us/car-rental/heraklion/heraklion-airport”,
],
},
{
“name”: “BSP-auto.com”,
“strategy_hint”: “B_search_scan”,
“candidates”: [
f”https://www.bsp-auto.com/en/search?”
f”location=Heraklion+Airport&from={PU_STR}&to={DO_STR}”
f”&pickup_time={PU_TIME}&dropoff_time={DO_TIME}”,
],
},
{
“name”: “StressFreeCarRental.com”,
“strategy_hint”: “B_search_scan”,
“candidates”: [
f”https://www.stressfreecarrental.com/search?”
f”location=Heraklion+Airport&from={PU_STR}T{PU_TIME}”
f”&to={DO_STR}T{DO_TIME}&age={DRIVER_AGE}”,
],
},
{
“name”: “otoQ.rent”,
“strategy_hint”: “Direct_brand_site”,
“candidates”: [
f”https://otoq.rent/”,
f”https://www.otoq.rent/”,
],
},
{
“name”: “Drive365.rent”,
“strategy_hint”: “Direct_brand_site”,
“candidates”: [
f”https://drive365.rent/”,
f”https://www.drive365.rent/”,
],
},
]

# ─── Core probe routine ───────────────────────────────────────────────

def probe_url(page, url, wait_ms=8000):
“””
Navigate to `url` with Playwright, wait for network to settle,
return a dict of observations.
“””
result = {
“url_tried”:       url,
“final_url”:       “”,
“http_status”:     0,
“load_time_ms”:    0,
“html_bytes”:      0,
“challenge”:       “none”,     # cloudflare | datadome | captcha | none
“brand_found”:     {},         # {brand_name: {found: bool, selector: str|None, snippet: str}}
“xhr_endpoints”:   [],         # list of {url, method, status, is_json}
“console_errors”:  [],
“screenshot_b64”:  “”,         # small screenshot thumbnail for visual debug
“error”:           “”,
}

```
# Track XHR/fetch network calls that return JSON (candidate supplier APIs)
xhr_calls = []
def on_response(response):
    try:
        ct = response.headers.get("content-type", "")
        req = response.request
        if req.resource_type in ("xhr", "fetch") and response.status < 400:
            xhr_calls.append({
                "url":     response.url[:200],
                "method":  req.method,
                "status":  response.status,
                "is_json": "json" in ct.lower(),
                "bytes":   int(response.headers.get("content-length", 0) or 0),
            })
    except Exception:
        pass
page.on("response", on_response)

# Track console errors
console_errs = []
page.on("console", lambda msg: console_errs.append(msg.text[:200]) if msg.type == "error" else None)

try:
    t0 = time.time()
    resp = page.goto(url, wait_until="domcontentloaded", timeout=30000)
    result["http_status"] = resp.status if resp else 0
    result["final_url"]   = page.url

    # Let JS finish — wait for network idle up to wait_ms
    try:
        page.wait_for_load_state("networkidle", timeout=wait_ms)
    except PWTimeout:
        pass  # Some sites never idle (polling); that's fine

    result["load_time_ms"] = int((time.time() - t0) * 1000)

    # Grab rendered HTML
    html = page.content()
    result["html_bytes"] = len(html)

    # Challenge detection
    low = html.lower()
    if "cf-challenge" in low or "checking your browser" in low or "cf_chl_" in low:
        result["challenge"] = "cloudflare"
    elif "datadome" in low or "dd_cookie" in low:
        result["challenge"] = "datadome"
    elif "captcha" in low or "g-recaptcha" in low or "h-captcha" in low:
        result["challenge"] = "captcha"

    # Brand hunt — for each brand, try variants, find the closest DOM element
    for brand in BRANDS:
        found_info = {"found": False, "selector": None, "snippet": ""}
        variants = [brand] + BRAND_VARIANTS.get(brand, [])
        for variant in variants:
            # Case-insensitive substring check on rendered text
            try:
                locator = page.locator(f"text=/{variant}/i").first
                if locator.count() > 0:
                    # Get the element's tag + class for mapping in Step 2
                    info = locator.evaluate("""el => ({
                        tag: el.tagName,
                        cls: (el.className || '').toString().slice(0,120),
                        id:  el.id || '',
                        text: (el.innerText || el.textContent || '').slice(0,150)
                    })""")
                    found_info = {
                        "found":    True,
                        "selector": f"{info['tag']}{('.' + info['cls'].split()[0]) if info['cls'] else ''}{('#' + info['id']) if info['id'] else ''}",
                        "snippet":  info["text"].replace("\n", " ").strip(),
                    }
                    break
            except Exception:
                continue
        result["brand_found"][brand] = found_info

    # XHR endpoints — keep only JSON ones, top 8 by size
    json_xhrs = [x for x in xhr_calls if x["is_json"]]
    json_xhrs.sort(key=lambda x: x["bytes"], reverse=True)
    result["xhr_endpoints"] = json_xhrs[:8]

    result["console_errors"] = console_errs[:5]

    # Thumbnail screenshot (very small, JPEG, base64) — for visual reference
    try:
        shot = page.screenshot(type="jpeg", quality=30, full_page=False)
        import base64 as _b64
        b64 = _b64.b64encode(shot[:80000]).decode("ascii")  # cap ~80KB
        result["screenshot_b64"] = f"data:image/jpeg;base64,{b64[:200]}...(truncated)"
    except Exception:
        pass

except PWTimeout as e:
    result["error"] = f"TIMEOUT: {str(e)[:150]}"
except Exception as e:
    result["error"] = f"{type(e).__name__}: {str(e)[:150]}"
finally:
    # Remove listener to avoid leaks on page reuse
    try:
        page.remove_listener("response", on_response)
    except Exception:
        pass

return result
```

def classify_strategy(result):
“””
Given a probe result, propose the Step-2 strategy for this broker.
“””
if result[“error”]:
return “UNREACHABLE”, “Network or timeout error; manual investigation needed”

```
if result["http_status"] >= 400:
    return "BAD_URL", f"HTTP {result['http_status']} — candidate URL is wrong"

if result["challenge"] != "none":
    return "BLOCKED_BY_" + result["challenge"].upper(), \
           f"Page served a {result['challenge']} challenge; Step 2 needs stealth tactics"

# Any brand found?
any_brand = any(b["found"] for b in result["brand_found"].values())
has_json_xhr = len(result["xhr_endpoints"]) > 0

if any_brand and has_json_xhr:
    return "READY_JSON_API", \
           f"Brand visible AND JSON XHR available — Step 2 can use direct API calls (fastest)"
if any_brand:
    return "READY_DOM_SCAN", \
           f"Brand visible in rendered DOM — Step 2 uses Playwright + selector scan"
if has_json_xhr:
    return "NEEDS_API_INSPECTION", \
           f"No brand text but {len(result['xhr_endpoints'])} JSON XHRs — manually inspect payloads"

# Fell through: page loaded, no challenge, but brand not detected
if result["html_bytes"] < 20000:
    return "EMPTY_PAGE", "Page loaded but HTML is tiny — likely SPA shell; Step 2 needs longer wait or interaction"
return "BRAND_ABSENT", \
       "Page rendered fully, no challenge, but brand not found — broker genuinely does not list us, OR filter needs to be clicked open"
```

# ─── Google Sheets writer ─────────────────────────────────────────────

def open_diagnostics_sheet(creds_json_str, spreadsheet_id):
creds_info = json.loads(creds_json_str)
scopes = [“https://www.googleapis.com/auth/spreadsheets”,
“https://www.googleapis.com/auth/drive”]
creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
gc = gspread.authorize(creds)
sh = gc.open_by_key(spreadsheet_id)

```
# Remove old Recon_Diagnostics if exists, create fresh
try:
    old = sh.worksheet("Recon_Diagnostics")
    sh.del_worksheet(old)
except gspread.WorksheetNotFound:
    pass

ws = sh.add_worksheet(title="Recon_Diagnostics", rows=300, cols=18)
headers = [
    "Timestamp (UTC)", "Broker", "Strategy hint", "URL tried",
    "HTTP status", "Final URL", "Load time (ms)", "HTML bytes",
    "Challenge", "otoQ found?", "otoQ selector", "Drive365 found?",
    "Drive365 selector", "JSON XHR count", "Top XHR sample",
    "Console errors", "Error", "→ Step 2 verdict",
]
ws.append_row(headers, value_input_option="USER_ENTERED")
return ws
```

def write_row(ws, broker_name, strategy_hint, result, verdict):
def fmt_brand(b):
bf = result[“brand_found”].get(b, {})
if not bf.get(“found”):
return “✖”
return f”✔ [{bf[‘snippet’][:40]}]”

```
def fmt_selector(b):
    bf = result["brand_found"].get(b, {})
    return bf.get("selector") or ""

top_xhr = ""
if result["xhr_endpoints"]:
    x = result["xhr_endpoints"][0]
    top_xhr = f"{x['method']} {x['url'][:120]} ({x['bytes']}B)"

row = [
    datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    broker_name,
    strategy_hint,
    result["url_tried"][:250],
    result["http_status"],
    result["final_url"][:250],
    result["load_time_ms"],
    result["html_bytes"],
    result["challenge"],
    fmt_brand("otoQ"),
    fmt_selector("otoQ")[:100],
    fmt_brand("Drive365"),
    fmt_selector("Drive365")[:100],
    len(result["xhr_endpoints"]),
    top_xhr,
    " | ".join(result["console_errors"])[:200],
    result["error"][:200],
    verdict,
]
ws.append_row(row, value_input_option="USER_ENTERED")
```

# ─── Main ─────────────────────────────────────────────────────────────

def main():
creds = os.getenv(“GOOGLE_SHEETS_CREDENTIALS”)
sid   = os.getenv(“SPREADSHEET_ID”)
if not creds or not sid:
print(“FATAL: GOOGLE_SHEETS_CREDENTIALS and SPREADSHEET_ID env vars required.”)
sys.exit(1)

```
print(f"[recon] Next-week weekday window: {PU_STR} → {DO_STR}")
print(f"[recon] Test airport: {TEST_AIRPORT['city']} ({TEST_AIRPORT['iata']})")
print(f"[recon] Brokers to probe: {len(BROKERS)}")

ws = open_diagnostics_sheet(creds, sid)
print(f"[recon] Diagnostics tab created.")

total_ok = 0
total_fail = 0

with sync_playwright() as pw:
    browser = pw.chromium.launch(
        headless=True,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ],
    )
    context = browser.new_context(
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"),
        viewport={"width": 1366, "height": 820},
        locale="en-US",
        timezone_id="Europe/Athens",
    )

    for broker in BROKERS:
        name = broker["name"]
        hint = broker["strategy_hint"]
        print(f"\n[recon] ── {name} ({hint}) ──")

        best_result = None
        best_verdict = None

        for candidate_url in broker["candidates"]:
            page = context.new_page()
            print(f"[recon]   trying: {candidate_url[:120]}")
            try:
                result = probe_url(page, candidate_url)
            except Exception as e:
                result = {
                    "url_tried": candidate_url, "final_url": "", "http_status": 0,
                    "load_time_ms": 0, "html_bytes": 0, "challenge": "none",
                    "brand_found": {b: {"found": False, "selector": None, "snippet": ""} for b in BRANDS},
                    "xhr_endpoints": [], "console_errors": [],
                    "screenshot_b64": "", "error": f"PROBE_CRASH: {e}",
                }
            verdict_code, verdict_detail = classify_strategy(result)
            verdict = f"{verdict_code} — {verdict_detail}"

            print(f"[recon]   → {verdict_code}")

            # Keep the "best" result: prefer READY_* over anything else
            rank = {
                "READY_JSON_API": 5, "READY_DOM_SCAN": 4,
                "NEEDS_API_INSPECTION": 3, "BRAND_ABSENT": 2,
                "EMPTY_PAGE": 2, "BLOCKED_BY_CLOUDFLARE": 1,
                "BLOCKED_BY_DATADOME": 1, "BLOCKED_BY_CAPTCHA": 1,
                "BAD_URL": 0, "UNREACHABLE": 0,
            }.get(verdict_code, 0)

            current_rank = -1
            if best_verdict:
                best_code = best_verdict.split(" — ")[0]
                current_rank = {
                    "READY_JSON_API": 5, "READY_DOM_SCAN": 4,
                    "NEEDS_API_INSPECTION": 3, "BRAND_ABSENT": 2,
                    "EMPTY_PAGE": 2, "BLOCKED_BY_CLOUDFLARE": 1,
                    "BLOCKED_BY_DATADOME": 1, "BLOCKED_BY_CAPTCHA": 1,
                    "BAD_URL": 0, "UNREACHABLE": 0,
                }.get(best_code, 0)

            if best_result is None or rank > current_rank:
                best_result = result
                best_verdict = verdict

            # Also log the attempt itself (every candidate gets a row)
            write_row(ws, name, hint, result, verdict)

            page.close()

            # Short polite delay between candidates
            time.sleep(1.5)

        # Summary tracking
        best_code = best_verdict.split(" — ")[0]
        if best_code.startswith("READY"):
            total_ok += 1
        else:
            total_fail += 1

    context.close()
    browser.close()

# Final summary row
ws.append_row([""] * 18)
ws.append_row([
    datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    "── SUMMARY ──", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
    f"READY: {total_ok} / {total_ok + total_fail}   |   NEEDS WORK: {total_fail}",
], value_input_option="USER_ENTERED")

print(f"\n[recon] Done. READY: {total_ok}, NEEDS WORK: {total_fail}")
print(f"[recon] Open the Google Sheet → 'Recon_Diagnostics' tab.")
```

if **name** == “**main**”:
try:
main()
except Exception:
traceback.print_exc()
sys.exit(1)
