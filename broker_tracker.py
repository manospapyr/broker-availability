#!/usr/bin/env python3
"""
Broker Availability Tracker
Checks car rental broker websites for availability of otoQ and Drive365
across their respective operating areas, and outputs results to an Excel file.
"""

import requests
import time
import re
import json
import os
from datetime import datetime, timedelta
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ──────────────────────────────────────────────────────────────────────
# CONFIGURATION: Brands, areas, brokers
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
# SEARCH FUNCTIONS
# ──────────────────────────────────────────────────────────────────────

# Location name mappings for search queries
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

# Brand name as it appears on broker sites
BRAND_SEARCH_NAMES = {
    "otoQ": ["otoQ", "otoq", "OTOQ", "Oto Q"],
    "Drive365": ["Drive365", "drive365", "DRIVE365", "Drive 365"],
}


def check_broker_availability(broker, brand, location):
    """
    Check if a broker lists a brand at a given location.
    
    Returns:
        "✔" if available
        "✖" if checked and not available
        "N/A" if the check could not be completed (error, timeout, etc.)
    """
    search_location = LOCATION_SEARCH_NAMES.get(location, location)
    brand_names = BRAND_SEARCH_NAMES.get(brand, [brand])
    
    # Build the search URL based on the broker
    broker_lower = broker.lower().replace(" ", "")
    
    try:
        # Use a pickup date 30 days from now, return 37 days from now
        pickup = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
        dropoff = (datetime.now() + timedelta(days=37)).strftime("%Y-%m-%d")
        
        # Different URL patterns per broker
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
        
        # Check if any brand name variant appears in the page
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


def build_broker_url(broker, location, pickup, dropoff):
    """Build the search URL for a specific broker."""
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
        "otoq.rent": f"https://otoq.rent",
        "drive365.rent": f"https://drive365.rent",
        "aurum": f"https://www.aurumcars.de/search?location={location_encoded}&pickup={pickup}&dropoff={dropoff}",
    }
    
    key = broker.lower().replace(" ", "")
    return broker_urls.get(key, None)


# ──────────────────────────────────────────────────────────────────────
# EXCEL OUTPUT
# ──────────────────────────────────────────────────────────────────────

def write_excel(otoq_results, drive365_results, filename="Broker_Availability_Tracker.xlsx"):
    """Write results to Excel matching the reference spreadsheet format."""
    wb = Workbook()
    
    # --- Sheet 1: otoQ ---
    ws1 = wb.active
    ws1.title = "otoQ"
    write_brand_sheet(ws1, "otoQ", OTOQ_AREAS, OTOQ_BROKERS, otoq_results)
    
    # --- Sheet 2: Drive365 ---
    ws2 = wb.create_sheet("Drive365")
    write_brand_sheet(ws2, "DRIVE365", DRIVE365_AREAS, DRIVE365_BROKERS, drive365_results)
    
    wb.save(filename)
    print(f"Results saved to {filename}")


def write_brand_sheet(ws, brand_label, areas, brokers, results):
    """Write a single brand sheet in the reference format."""
    
    # Styles
    header_font = Font(bold=True, size=11, name="Arial")
    brand_font = Font(bold=True, size=14, name="Arial")
    country_font = Font(bold=True, size=11, name="Arial")
    city_font = Font(italic=True, size=10, name="Arial")
    cell_font = Font(size=10, name="Arial")
    center = Alignment(horizontal="center", vertical="center")
    left = Alignment(horizontal="left", vertical="center")
    
    green_fill = PatternFill("solid", fgColor="C6EFCE")
    red_fill = PatternFill("solid", fgColor="FFC7CE")
    grey_fill = PatternFill("solid", fgColor="D9D9D9")
    header_fill = PatternFill("solid", fgColor="4472C4")
    header_font_white = Font(bold=True, size=11, name="Arial", color="FFFFFF")
    
    thin_border = Border(
        left=Side(style="thin"),
        right=Side(style="thin"),
        top=Side(style="thin"),
        bottom=Side(style="thin"),
    )
    
    # Build flat list of cities with their countries
    cities = []
    country_spans = []  # (country_name, start_col, end_col)
    col = 2  # Column B onwards (A is for broker names)
    for country, city_list in areas.items():
        start = col
        for city in city_list:
            cities.append((country, city))
            col += 1
        country_spans.append((country, start, col - 1))
    
    total_cols = col - 1
    
    # Row 1: Brand name
    ws.cell(row=1, column=1, value=brand_label).font = brand_font
    
    # Row 2: Country header row
    for country, start, end in country_spans:
        cell = ws.cell(row=2, column=start, value=country)
        cell.font = header_font_white
        cell.fill = header_fill
        cell.alignment = center
        cell.border = thin_border
        if start != end:
            ws.merge_cells(start_row=2, start_column=start, end_row=2, end_column=end)
        # Fill merged cells border
        for c in range(start, end + 1):
            ws.cell(row=2, column=c).border = thin_border
            ws.cell(row=2, column=c).fill = header_fill
    
    # Row 3: City header row
    for idx, (country, city) in enumerate(cities):
        c = idx + 2
        cell = ws.cell(row=3, column=c, value=city)
        cell.font = city_font
        cell.alignment = center
        cell.border = thin_border
    
    # Header for broker column
    ws.cell(row=2, column=1, value="").border = thin_border
    ws.cell(row=3, column=1, value="").border = thin_border
    
    # Data rows (brokers)
    for b_idx, broker in enumerate(brokers):
        row = 4 + b_idx
        cell = ws.cell(row=row, column=1, value=broker)
        cell.font = cell_font
        cell.alignment = left
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
            else:  # N/A
                cell.fill = grey_fill
    
    # Column widths
    ws.column_dimensions["A"].width = 28
    for c in range(2, total_cols + 1):
        ws.column_dimensions[get_column_letter(c)].width = 14


# ──────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────

def run_checks(brand, areas, brokers):
    """Run all availability checks for a brand."""
    results = {}
    total_checks = sum(len(city_list) for city_list in areas.values()) * len(brokers)
    done = 0
    
    for broker in brokers:
        results[broker] = {}
        for country, city_list in areas.items():
            for city in city_list:
                done += 1
                print(f"  [{done}/{total_checks}] {broker} → {city} ({country})...")
                result = check_broker_availability(broker, brand, city)
                results[broker][city] = result
                time.sleep(1)  # Be polite with rate limiting
    
    return results


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
    
    print(f"\n--- Writing results to {output_file} ---")
    write_excel(otoq_results, drive365_results, output_file)
    
    print("\nDone!")


if __name__ == "__main__":
    main()
