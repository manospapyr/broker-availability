#!/usr/bin/env python3
"""
Broker Availability Tracker
Checks car rental broker websites for availability of otoQ and Drive365
across their respective operating areas, and writes results to Google Sheets.
"""

import requests
import time
import json
import os
import sys
from datetime import datetime, timedelta

import gspread
from google.oauth2.service_account import Credentials
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ──────────────────────────────────────────────────────────────────────
# CONFIGURATION: Brands → areas → cities (per brand, no cross-brand)
# ──────────────────────────────────────────────────────────────────────

OTOQ_AREAS = {
    "Greece": ["Athens", "Zante", "Chania", "Heraklion"],
    "Malta": ["Valletta"],
    "Albania": ["Tirana"],
    "Tunisia": ["Tunis", "Enfidha", "Monastir", "Djerba"],
    "United States": ["Orlando", "Miami", "Tampa", "Hollywood"],
    "Morocco": ["Rabat", "Fez", "Tangier", "Agadir", "Marrakesh", "Casablanca"],
    "Montenegro": ["Podgorica"],
    "Romania": ["Timisoara"],
    "Mauritius": ["Plaisance"],
}

# Drive365 does NOT operate in Tunisia, Morocco, Romania, or Mauritius
DRIVE365_AREAS = {
    "Greece": ["Heraklion", "Athens"],
    "Albania": ["Tirana"],
    "United States": ["Miami", "Tampa", "Hollywood", "Orlando"],
    "Malta": ["Valletta"],
    "Montenegro": ["Podgorica", "Tivat"],
}

OTOQ_BROKERS = [
    "Discovercars.com",
    "Qeeq.com",
    "Orbitcarhire.com",
    "carrental.hotelbeds.com",
    "Enjoytravel.com",
    "Aurumcars.de",
    "Carjet.com",
    "Rentcars.com/en",
    "CarFlexi.com",
    "Economybookings.com",
    "Priceline.com/rental-cars",
    "Rentcarla.com",
    "Vipcars.com",
    "Yolcu360",
    "Wisecars.com",
    "BSP-auto.com",
    "StressFreeCarRental.com",
    "otoQ.rent",
]

DRIVE365_BROKERS = [
    "Discovercars.com",
    "Orbitcarhire.com",
    "Vipcars.com",
    "Enjoytravel.com",
    "Carjet.com",
    "EconomyBookings.com",
    "bsp-auto.com",
    "Aurum",
    "StressFreeCarRental.com",
    "Drive365.rent",
]

# ──────────────────────────────────────────────────────────────────────
# LOCATION & BRAND SEARCH HELPERS
# ──────────────────────────────────────────────────────────────────────

LOCATION_SEARCH_NAMES = {
    "Athens": "Athens Airport",
    "Zante": "Zakynthos Airport",
    "Chania": "Chania Airport",
    "Heraklion": "Heraklion Airport",
    "Valletta": "Malta Airport",
    "Tirana": "Tirana Airport",
    "Tunis": "Tunis Airport",
    "Enfidha": "Enfidha Airport",
    "Monastir": "Monastir Airport",
    "Djerba": "Djerba Airport",
    "Orlando": "Orlando Airport",
    "Miami": "Miami Airport",
    "Tampa": "Tampa Airport",
    "Hollywood": "Fort Lauderdale Airport",
    "Rabat": "Rabat Airport",
    "Fez": "Fez Airport",
    "Tangier": "Tangier Airport",
    "Agadir": "Agadir Airport",
    "Marrakesh": "Marrakech Airport",
    "Casablanca": "Casablanca Airport",
    "Podgorica": "Podgorica Airport",
    "Timisoara": "Timisoara Airport",
    "Plaisance": "Mauritius Airport",
    "Tivat": "Tivat Airport",
}

BRAND_SEARCH_NAMES = {
    "otoQ": ["otoQ", "otoq", "OTOQ", "Oto Q"],
    "Drive365": ["Drive365", "drive365", "DRIVE365", "Drive 365"],
}


# ──────────────────────────────────────────────────────────────────────
# BROKER URL BUILDER
# ──────────────────────────────────────────────────────────────────────

def build_broker_url(broker, location, pickup, dropoff):
    location_encoded = requests.utils.quote(location)
    broker_urls = {
        "discovercars.com": f"https://www.discovercars.com/search?location={location_encoded}&pickup={pickup}&dropoff={dropoff}",
        "qeeq.com": f"https://www.qeeq.com/car-rental?location={location_encoded}&pickup={pickup}&dropoff={dropoff}",
        "orbitcarhire.com": f"https://www.orbitcarhire.com/search?location={location_encoded}&from={pickup}&to={dropoff}",
        "carrental.hotelbeds.com": f"https://carrental.hotelbeds.com/search?location={location_encoded}&pickup={pickup}&dropoff={dropoff}",
        "enjoytravel.com": f"https://www.enjoytravel.com/en/car-rental?location={location_encoded}&from={pickup}&to={dropoff}",
        "aurumcars.de": f"https://www.aurumcars.de/search?location={location_encoded}&pickup={pickup}&dropoff={dropoff}",
        "carjet.com": f"https://www.carjet.com/search?location={location_encoded}&pickup={pickup}&dropoff={dropoff}",
        "rentcars.com/en": f"https://www.rentcars.com/en/search?location={location_encoded}&from={pickup}&to={dropoff}",
        "carflexi.com": f"https://www.carflexi.com/search?location={location_encoded}&pickup={pickup}&dropoff={dropoff}",
        "economybookings.com": f"https://www.economybookings.com/search?location={location_encoded}&pickup={pickup}&dropoff={dropoff}",
        "priceline.com/rental-cars": f"https://www.priceline.com/rental-cars/search?location={location_encoded}&pickup={pickup}&dropoff={dropoff}",
        "rentcarla.com": f"https://www.rentcarla.com/search?location={location_encoded}&from={pickup}&to={dropoff}",
        "vipcars.com": f"https://www.vipcars.com/search?location={location_encoded}&pickup={pickup}&dropoff={dropoff}",
        "yolcu360": f"https://www.yolcu360.com/en/search?location={location_encoded}&pickup={pickup}&dropoff={dropoff}",
        "wisecars.com": f"https://www.wisecars.com/search?location={location_encoded}&pickup={pickup}&dropoff={dropoff}",
        "bsp-auto.com": f"https://www.bsp-auto.com/search?location={location_encoded}&pickup={pickup}&dropoff={dropoff}",
        "stressfreecarrental.com": f"https://www.stressfreecarrental.com/search?location={location_encoded}&from={pickup}&to={dropoff}",
        "otoq.rent": "https://otoq.rent",
        "drive365.rent": "https://drive365.rent",
        "aurum": f"https://www.aurumcars.de/search?location={location_encoded}&pickup={pickup}&dropoff={dropoff}",
    }
    key = broker.lower().replace(" ", "")
    return broker_urls.get(key, None)


# ──────────────────────────────────────────────────────────────────────
# AVAILABILITY CHECK
# ──────────────────────────────────────────────────────────────────────

def check_broker_availability(broker, brand, location):
    """
    Returns:
        "✔" if the brand is found on the broker for that location
        "✖" if checked successfully but brand not found
        "N/A" if the check could not be completed
    """
    search_location = LOCATION_SEARCH_NAMES.get(location, location)
    brand_names = BRAND_SEARCH_NAMES.get(brand, [brand])

    try:
        pickup = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
        dropoff = (datetime.now() + timedelta(days=37)).strftime("%Y-%m-%d")
        url = build_broker_url(broker, search_location, pickup, dropoff)
        if url is None:
            return "N/A"

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
        response = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
        if response.status_code != 200:
            return "N/A"

        page_content = response.text.lower()
        for name in brand_names:
            if name.lower() in page_content:
                return "✔"
        return "✖"

    except requests.exceptions.Timeout:
        return "N/A"
    except requests.exceptions.ConnectionError:
        return "N/A"
    except Exception as e:
        print(f"  Error checking {broker} for {brand} at {location}: {e}")
        return "N/A"


# ──────────────────────────────────────────────────────────────────────
# RUN ALL CHECKS FOR A BRAND
# ──────────────────────────────────────────────────────────────────────

def run_checks(brand, areas, brokers):
    results = {}
    total = sum(len(cl) for cl in areas.values()) * len(brokers)
    done = 0
    for broker in brokers:
        results[broker] = {}
        for country, city_list in areas.items():
            for city in city_list:
                done += 1
                print(f"  [{done}/{total}] {broker} → {city} ({country})...")
                results[broker][city] = check_broker_availability(broker, brand, city)
                time.sleep(1)
    return results


# ──────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS WRITER  ← THIS IS THE KEY MISSING PIECE
# ──────────────────────────────────────────────────────────────────────

def get_gspread_client():
    creds_json = os.environ.get("GOOGLE_SHEETS_CREDENTIALS")
    if not creds_json:
        print("WARNING: GOOGLE_SHEETS_CREDENTIALS not set, skipping Sheets update")
        return None
    creds_dict = json.loads(creds_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(credentials)


def build_sheet_data(brand_label, areas, brokers, results):
    """Build a 2D list matching the reference spreadsheet layout."""
    cities = []
    for country, city_list in areas.items():
        for city in city_list:
            cities.append((country, city))

    # Row 1: brand label
    row1 = [brand_label] + [""] * len(cities)

    # Row 2: country headers (name at first city of each country, blank for rest)
    row2 = [""]
    prev_country = None
    for country, city in cities:
        if country != prev_country:
            row2.append(country)
            prev_country = country
        else:
            row2.append("")

    # Row 3: city names
    row3 = [""] + [city for _, city in cities]

    # Data rows: broker name + ✔/✖/N/A per city
    data_rows = []
    for broker in brokers:
        row = [broker]
        for _, city in cities:
            row.append(results.get(broker, {}).get(city, "N/A"))
        data_rows.append(row)

    return [row1, row2, row3] + data_rows


def update_google_sheets(otoq_results, drive365_results):
    client = get_gspread_client()
    if client is None:
        return

    spreadsheet_id = os.environ.get("SPREADSHEET_ID")
    if not spreadsheet_id:
        print("WARNING: SPREADSHEET_ID not set, skipping Sheets update")
        return

    try:
        sh = client.open_by_key(spreadsheet_id)
    except Exception as e:
        print(f"ERROR: Could not open spreadsheet: {e}")
        return

    # --- otoQ sheet ---
    otoq_data = build_sheet_data("otoQ", OTOQ_AREAS, OTOQ_BROKERS, otoq_results)
    try:
        ws = sh.worksheet("otoQ")
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title="otoQ", rows=len(otoq_data) + 5, cols=len(otoq_data[0]) + 5)
    ws.update(range_name="A1", values=otoq_data)
    print("  ✔ Updated 'otoQ' sheet")

    # --- Drive365 sheet ---
    d365_data = build_sheet_data("DRIVE365", DRIVE365_AREAS, DRIVE365_BROKERS, drive365_results)
    try:
        ws = sh.worksheet("Drive365")
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title="Drive365", rows=len(d365_data) + 5, cols=len(d365_data[0]) + 5)
    ws.update(range_name="A1", values=d365_data)
    print("  ✔ Updated 'Drive365' sheet")


# ──────────────────────────────────────────────────────────────────────
# LOCAL EXCEL WRITER (backup copy)
# ──────────────────────────────────────────────────────────────────────

def write_brand_sheet(ws, brand_label, areas, brokers, results):
    header_font_white = Font(bold=True, size=11, name="Arial", color="FFFFFF")
    city_font = Font(italic=True, size=10, name="Arial")
    cell_font = Font(size=10, name="Arial")
    brand_font = Font(bold=True, size=14, name="Arial")
    center = Alignment(horizontal="center", vertical="center")
    left_align = Alignment(horizontal="left", vertical="center")
    green_fill = PatternFill("solid", fgColor="C6EFCE")
    red_fill = PatternFill("solid", fgColor="FFC7CE")
    grey_fill = PatternFill("solid", fgColor="D9D9D9")
    header_fill = PatternFill("solid", fgColor="4472C4")
    thin_border = Border(
        left=Side(style="thin"), right=Side(style="thin"),
        top=Side(style="thin"), bottom=Side(style="thin"),
    )

    cities = []
    country_spans = []
    col = 2
    for country, city_list in areas.items():
        start = col
        for city in city_list:
            cities.append((country, city))
            col += 1
        country_spans.append((country, start, col - 1))
    total_cols = col - 1

    ws.cell(row=1, column=1, value=brand_label).font = brand_font
    for country, start, end in country_spans:
        cell = ws.cell(row=2, column=start, value=country)
        cell.font = header_font_white
        cell.fill = header_fill
        cell.alignment = center
        cell.border = thin_border
        if start != end:
            ws.merge_cells(start_row=2, start_column=start, end_row=2, end_column=end)
        for c in range(start, end + 1):
            ws.cell(row=2, column=c).border = thin_border
            ws.cell(row=2, column=c).fill = header_fill

    for idx, (country, city) in enumerate(cities):
        c = idx + 2
        cell = ws.cell(row=3, column=c, value=city)
        cell.font = city_font
        cell.alignment = center
        cell.border = thin_border

    for b_idx, broker in enumerate(brokers):
        row = 4 + b_idx
        cell = ws.cell(row=row, column=1, value=broker)
        cell.font = cell_font
        cell.alignment = left_align
        cell.border = thin_border
        for c_idx, (country, city) in enumerate(cities):
            col_num = c_idx + 2
            value = results.get(broker, {}).get(city, "N/A")
            cell = ws.cell(row=row, column=col_num, value=value)
            cell.font = cell_font
            cell.alignment = center
            cell.border = thin_border
            if value == "✔":
                cell.fill = green_fill
            elif value == "✖":
                cell.fill = red_fill
            else:
                cell.fill = grey_fill

    ws.column_dimensions["A"].width = 28
    for c in range(2, total_cols + 1):
        ws.column_dimensions[get_column_letter(c)].width = 14


def write_excel(otoq_results, drive365_results, filename="Broker_Availability_Tracker.xlsx"):
    wb = Workbook()
    ws1 = wb.active
    ws1.title = "otoQ"
    write_brand_sheet(ws1, "otoQ", OTOQ_AREAS, OTOQ_BROKERS, otoq_results)
    ws2 = wb.create_sheet("Drive365")
    write_brand_sheet(ws2, "DRIVE365", DRIVE365_AREAS, DRIVE365_BROKERS, drive365_results)
    wb.save(filename)
    print(f"  ✔ Local Excel saved: {filename}")


# ──────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────

def main():
    output_file = os.environ.get("OUTPUT_FILE", "Broker_Availability_Tracker.xlsx")

    print("=" * 60)
    print("Broker Availability Tracker")
    print("=" * 60)

    print(f"\n--- Checking otoQ ({len(OTOQ_BROKERS)} brokers, "
          f"{sum(len(v) for v in OTOQ_AREAS.values())} locations) ---")
    otoq_results = run_checks("otoQ", OTOQ_AREAS, OTOQ_BROKERS)

    print(f"\n--- Checking Drive365 ({len(DRIVE365_BROKERS)} brokers, "
          f"{sum(len(v) for v in DRIVE365_AREAS.values())} locations) ---")
    drive365_results = run_checks("Drive365", DRIVE365_AREAS, DRIVE365_BROKERS)

    print(f"\n--- Writing local Excel to {output_file} ---")
    write_excel(otoq_results, drive365_results, output_file)

    print("\n--- Updating Google Sheets ---")
    update_google_sheets(otoq_results, drive365_results)

    print("\nDone!")


if __name__ == "__main__":
    main()
