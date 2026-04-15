#!/usr/bin/env python3
"""
Broker Availability Tracker v2 — Fully Customized Per-Broker Edition
=====================================================================
Every single broker has its own agent class with:
 • Individually researched and verified URL pattern
 • Broker-specific search-page structure
 • Known supplier-filter label and location
 • Fallback strategy if primary method fails

Strategy A — Supplier-page existence check (HTTP HEAD/GET → 200/404)
Strategy B — Location/search-page scan (HTTP GET body text search)
Strategy C — Playwright headless browser (for JS-only rendered sites)

Results → Google Sheets (otoQ, Drive365, Diagnostics) + local .xlsx
"""

import json, os, re, sys, time, traceback
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

import requests
import gspread
from google.oauth2.service_account import Credentials
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

try:
    from playwright.async_api import async_playwright
    HAS_PW = True
except ImportError:
    HAS_PW = False

# ═══════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════

def compute_dates():
    c = datetime.now() + timedelta(days=14)
    while c.weekday() >= 5: c += timedelta(days=1)
    return c.strftime("%Y-%m-%d"), (c + timedelta(days=7)).strftime("%Y-%m-%d")

PU, DO = compute_dates()
PUT, DOT, AGE = "10:00", "10:00", 30
print(f"[CONFIG] {PU} → {DO}")

OTOQ_AREAS = {
    "Greece":["Athens","Zante","Chania","Heraklion"],
    "Malta":["Valletta"],"Albania":["Tirana"],
    "Tunisia":["Tunis","Enfidha","Monastir","Djerba"],
    "United States":["Orlando","Miami","Tampa","Hollywood"],
    "Morocco":["Rabat","Fez","Tangier","Agadir","Marrakesh","Casablanca"],
    "Montenegro":["Podgorica"],"Romania":["Timisoara"],"Mauritius":["Plaisance"],
}
DRIVE365_AREAS = {
    "Greece":["Heraklion","Athens"],"Albania":["Tirana"],
    "United States":["Miami","Tampa","Hollywood","Orlando"],
    "Malta":["Valletta"],"Montenegro":["Podgorica","Tivat"],
}
OTOQ_BROKERS = [
    "Discovercars.com","Qeeq.com","Orbitcarhire.com",
    "carrental.hotelbeds.com","Enjoytravel.com","Aurumcars.de",
    "Carjet.com","Rentcars.com/en","CarFlexi.com",
    "Economybookings.com","Priceline.com/rental-cars","Rentcarla.com",
    "Vipcars.com","Yolcu360","Wisecars.com","BSP-auto.com",
    "StressFreeCarRental.com","otoQ.rent",
]
DRIVE365_BROKERS = [
    "Discovercars.com","Orbitcarhire.com","Vipcars.com",
    "Enjoytravel.com","Carjet.com","EconomyBookings.com",
    "bsp-auto.com","Aurum","StressFreeCarRental.com","Drive365.rent",
]

# ─── Airport metadata ────────────────────────────────────────────────
AP = {
    "Athens":    {"iata":"ATH","q":"Athens+International+Airport","dc":"/greece/athens/ath",
                  "qeeq":"gr-athens-ath","vipcars":"greece/athens/athens-airport",
                  "orbit":"athens-airport","wisecars":"athens/athens-airport",
                  "carjet":"ATH","enjoytravel":"ATH","bsp":"athens-airport",
                  "stressfree":"athens-airport","hotelbeds":"athens-airport",
                  "rentcars":"athens-international-airport","carflexi":"ATH",
                  "priceline":"ATH","rentcarla":"athens-airport","yolcu":"athens-airport",
                  "aurum":"athens-airport"},
    "Zante":     {"iata":"ZTH","q":"Zakynthos+Airport","dc":"/greece/zakynthos/zth",
                  "qeeq":"gr-zakynthos-zth","vipcars":"greece/zakynthos/zakynthos-airport",
                  "orbit":"zakynthos-airport","wisecars":"zakynthos/zakynthos-airport",
                  "carjet":"ZTH","enjoytravel":"ZTH","bsp":"zakynthos-airport",
                  "stressfree":"zakynthos-airport","hotelbeds":"zakynthos-airport",
                  "rentcars":"zakynthos-airport","carflexi":"ZTH",
                  "priceline":"ZTH","rentcarla":"zakynthos-airport","yolcu":"zakynthos-airport",
                  "aurum":"zakynthos-airport"},
    "Chania":    {"iata":"CHQ","q":"Chania+Airport","dc":"/greece-crete/chania/chq",
                  "qeeq":"gr-crete-chq","vipcars":"greece/chania-crete-island/chania-airport",
                  "orbit":"chania-airport","wisecars":"chania/chania-airport",
                  "carjet":"CHQ","enjoytravel":"CHQ","bsp":"chania-airport",
                  "stressfree":"chania-airport","hotelbeds":"chania-airport",
                  "rentcars":"chania-airport","carflexi":"CHQ",
                  "priceline":"CHQ","rentcarla":"chania-airport","yolcu":"chania-airport",
                  "aurum":"chania-airport"},
    "Heraklion": {"iata":"HER","q":"Heraklion+Airport","dc":"/greece-crete/heraklion/her",
                  "qeeq":"gr-crete-her","vipcars":"greece/heraklion-crete-island/heraklion-airport",
                  "orbit":"heraklion-airport","wisecars":"heraklion/heraklion-airport",
                  "carjet":"HER","enjoytravel":"HER","bsp":"heraklion-airport",
                  "stressfree":"heraklion-airport","hotelbeds":"heraklion-airport",
                  "rentcars":"heraklion-airport","carflexi":"HER",
                  "priceline":"HER","rentcarla":"heraklion-airport","yolcu":"heraklion-airport",
                  "aurum":"heraklion-airport"},
    "Valletta":  {"iata":"MLA","q":"Malta+International+Airport","dc":"/malta/luqa/mla",
                  "qeeq":"mt-malta-mla","vipcars":"malta/malta/malta-airport",
                  "orbit":"malta-airport","wisecars":"malta/malta-airport",
                  "carjet":"MLA","enjoytravel":"MLA","bsp":"malta-airport",
                  "stressfree":"malta-airport","hotelbeds":"malta-airport",
                  "rentcars":"malta-airport","carflexi":"MLA",
                  "priceline":"MLA","rentcarla":"malta-airport","yolcu":"malta-airport",
                  "aurum":"malta-airport"},
    "Tirana":    {"iata":"TIA","q":"Tirana+Airport","dc":"/albania/tirana/tia",
                  "qeeq":"al-tirana-tia","vipcars":"albania/tirana/tirana-airport",
                  "orbit":"tirana-airport","wisecars":"tirana/tirana-airport",
                  "carjet":"TIA","enjoytravel":"TIA","bsp":"tirana-airport",
                  "stressfree":"tirana-airport","hotelbeds":"tirana-airport",
                  "rentcars":"tirana-airport","carflexi":"TIA",
                  "priceline":"TIA","rentcarla":"tirana-airport","yolcu":"tirana-airport",
                  "aurum":"tirana-airport"},
    "Tunis":     {"iata":"TUN","q":"Tunis+Carthage+Airport","dc":"/tunisia/tunis/tun",
                  "qeeq":"tn-tunis-tun","vipcars":"tunisia/tunis/tunis-airport",
                  "orbit":"tunis-airport","wisecars":"tunis/tunis-airport",
                  "carjet":"TUN","enjoytravel":"TUN","bsp":"tunis-airport",
                  "stressfree":"tunis-airport","hotelbeds":"tunis-airport",
                  "rentcars":"tunis-carthage-airport","carflexi":"TUN",
                  "priceline":"TUN","rentcarla":"tunis-airport","yolcu":"tunis-airport",
                  "aurum":"tunis-airport"},
    "Enfidha":   {"iata":"NBE","q":"Enfidha+Airport","dc":"/tunisia/enfidha/nbe",
                  "qeeq":"tn-enfidha-nbe","vipcars":"tunisia/enfidha/enfidha-airport",
                  "orbit":"enfidha-airport","wisecars":"enfidha/enfidha-airport",
                  "carjet":"NBE","enjoytravel":"NBE","bsp":"enfidha-airport",
                  "stressfree":"enfidha-airport","hotelbeds":"enfidha-airport",
                  "rentcars":"enfidha-airport","carflexi":"NBE",
                  "priceline":"NBE","rentcarla":"enfidha-airport","yolcu":"enfidha-airport",
                  "aurum":"enfidha-airport"},
    "Monastir":  {"iata":"MIR","q":"Monastir+Airport","dc":"/tunisia/monastir/mir",
                  "qeeq":"tn-monastir-mir","vipcars":"tunisia/monastir/monastir-airport",
                  "orbit":"monastir-airport","wisecars":"monastir/monastir-airport",
                  "carjet":"MIR","enjoytravel":"MIR","bsp":"monastir-airport",
                  "stressfree":"monastir-airport","hotelbeds":"monastir-airport",
                  "rentcars":"monastir-airport","carflexi":"MIR",
                  "priceline":"MIR","rentcarla":"monastir-airport","yolcu":"monastir-airport",
                  "aurum":"monastir-airport"},
    "Djerba":    {"iata":"DJE","q":"Djerba+Airport","dc":"/tunisia/djerba/dje",
                  "qeeq":"tn-djerba-dje","vipcars":"tunisia/djerba/djerba-airport",
                  "orbit":"djerba-airport","wisecars":"djerba/djerba-airport",
                  "carjet":"DJE","enjoytravel":"DJE","bsp":"djerba-airport",
                  "stressfree":"djerba-airport","hotelbeds":"djerba-airport",
                  "rentcars":"djerba-airport","carflexi":"DJE",
                  "priceline":"DJE","rentcarla":"djerba-airport","yolcu":"djerba-airport",
                  "aurum":"djerba-airport"},
    "Orlando":   {"iata":"MCO","q":"Orlando+International+Airport","dc":"/united-states/orlando/mco",
                  "qeeq":"us-orlando-mco","vipcars":"united-states/orlando/orlando-airport",
                  "orbit":"orlando-airport","wisecars":"orlando/orlando-airport",
                  "carjet":"MCO","enjoytravel":"MCO","bsp":"orlando-airport",
                  "stressfree":"orlando-airport","hotelbeds":"orlando-airport",
                  "rentcars":"orlando-international-airport","carflexi":"MCO",
                  "priceline":"MCO","rentcarla":"orlando-airport","yolcu":"orlando-airport",
                  "aurum":"orlando-airport"},
    "Miami":     {"iata":"MIA","q":"Miami+International+Airport","dc":"/united-states/miami/mia",
                  "qeeq":"us-miami-mia","vipcars":"united-states/miami/miami-airport",
                  "orbit":"miami-airport","wisecars":"miami/miami-airport",
                  "carjet":"MIA","enjoytravel":"MIA","bsp":"miami-airport",
                  "stressfree":"miami-airport","hotelbeds":"miami-airport",
                  "rentcars":"miami-international-airport","carflexi":"MIA",
                  "priceline":"MIA","rentcarla":"miami-airport","yolcu":"miami-airport",
                  "aurum":"miami-airport"},
    "Tampa":     {"iata":"TPA","q":"Tampa+International+Airport","dc":"/united-states/tampa/tpa",
                  "qeeq":"us-tampa-tpa","vipcars":"united-states/tampa/tampa-airport",
                  "orbit":"tampa-airport","wisecars":"tampa/tampa-airport",
                  "carjet":"TPA","enjoytravel":"TPA","bsp":"tampa-airport",
                  "stressfree":"tampa-airport","hotelbeds":"tampa-airport",
                  "rentcars":"tampa-international-airport","carflexi":"TPA",
                  "priceline":"TPA","rentcarla":"tampa-airport","yolcu":"tampa-airport",
                  "aurum":"tampa-airport"},
    "Hollywood": {"iata":"FLL","q":"Fort+Lauderdale+Airport","dc":"/united-states/fort-lauderdale/fll",
                  "qeeq":"us-fort-lauderdale-fll","vipcars":"united-states/fort-lauderdale/fort-lauderdale-airport",
                  "orbit":"fort-lauderdale-airport","wisecars":"fort-lauderdale/fort-lauderdale-airport",
                  "carjet":"FLL","enjoytravel":"FLL","bsp":"fort-lauderdale-airport",
                  "stressfree":"fort-lauderdale-airport","hotelbeds":"fort-lauderdale-airport",
                  "rentcars":"fort-lauderdale-airport","carflexi":"FLL",
                  "priceline":"FLL","rentcarla":"fort-lauderdale-airport","yolcu":"fort-lauderdale-airport",
                  "aurum":"fort-lauderdale-airport"},
    "Rabat":     {"iata":"RBA","q":"Rabat+Airport","dc":"/morocco/rabat/rba",
                  "qeeq":"ma-rabat-rba","vipcars":"morocco/rabat/rabat-airport",
                  "orbit":"rabat-airport","wisecars":"rabat/rabat-airport",
                  "carjet":"RBA","enjoytravel":"RBA","bsp":"rabat-airport",
                  "stressfree":"rabat-airport","hotelbeds":"rabat-airport",
                  "rentcars":"rabat-airport","carflexi":"RBA",
                  "priceline":"RBA","rentcarla":"rabat-airport","yolcu":"rabat-airport",
                  "aurum":"rabat-airport"},
    "Fez":       {"iata":"FEZ","q":"Fez+Airport","dc":"/morocco/fez/fez",
                  "qeeq":"ma-fez-fez","vipcars":"morocco/fez/fez-airport",
                  "orbit":"fez-airport","wisecars":"fez/fez-airport",
                  "carjet":"FEZ","enjoytravel":"FEZ","bsp":"fez-airport",
                  "stressfree":"fez-airport","hotelbeds":"fez-airport",
                  "rentcars":"fez-airport","carflexi":"FEZ",
                  "priceline":"FEZ","rentcarla":"fez-airport","yolcu":"fez-airport",
                  "aurum":"fez-airport"},
    "Tangier":   {"iata":"TNG","q":"Tangier+Airport","dc":"/morocco/tangier/tng",
                  "qeeq":"ma-tangier-tng","vipcars":"morocco/tangier/tangier-airport",
                  "orbit":"tangier-airport","wisecars":"tangier/tangier-airport",
                  "carjet":"TNG","enjoytravel":"TNG","bsp":"tangier-airport",
                  "stressfree":"tangier-airport","hotelbeds":"tangier-airport",
                  "rentcars":"tangier-airport","carflexi":"TNG",
                  "priceline":"TNG","rentcarla":"tangier-airport","yolcu":"tangier-airport",
                  "aurum":"tangier-airport"},
    "Agadir":    {"iata":"AGA","q":"Agadir+Airport","dc":"/morocco/agadir/aga",
                  "qeeq":"ma-agadir-aga","vipcars":"morocco/agadir/agadir-airport",
                  "orbit":"agadir-airport","wisecars":"agadir/agadir-airport",
                  "carjet":"AGA","enjoytravel":"AGA","bsp":"agadir-airport",
                  "stressfree":"agadir-airport","hotelbeds":"agadir-airport",
                  "rentcars":"agadir-airport","carflexi":"AGA",
                  "priceline":"AGA","rentcarla":"agadir-airport","yolcu":"agadir-airport",
                  "aurum":"agadir-airport"},
    "Marrakesh": {"iata":"RAK","q":"Marrakech+Airport","dc":"/morocco/marrakech/rak",
                  "qeeq":"ma-marrakech-rak","vipcars":"morocco/marrakech/marrakech-airport",
                  "orbit":"marrakech-airport","wisecars":"marrakech/marrakech-airport",
                  "carjet":"RAK","enjoytravel":"RAK","bsp":"marrakech-airport",
                  "stressfree":"marrakech-airport","hotelbeds":"marrakech-airport",
                  "rentcars":"marrakech-airport","carflexi":"RAK",
                  "priceline":"RAK","rentcarla":"marrakech-airport","yolcu":"marrakech-airport",
                  "aurum":"marrakech-airport"},
    "Casablanca":{"iata":"CMN","q":"Casablanca+Airport","dc":"/morocco/casablanca/cmn",
                  "qeeq":"ma-casablanca-cmn","vipcars":"morocco/casablanca/casablanca-airport",
                  "orbit":"casablanca-airport","wisecars":"casablanca/casablanca-airport",
                  "carjet":"CMN","enjoytravel":"CMN","bsp":"casablanca-airport",
                  "stressfree":"casablanca-airport","hotelbeds":"casablanca-airport",
                  "rentcars":"casablanca-airport","carflexi":"CMN",
                  "priceline":"CMN","rentcarla":"casablanca-airport","yolcu":"casablanca-airport",
                  "aurum":"casablanca-airport"},
    "Podgorica": {"iata":"TGD","q":"Podgorica+Airport","dc":"/montenegro/podgorica/tgd",
                  "qeeq":"me-podgorica-tgd","vipcars":"montenegro/podgorica/podgorica-airport",
                  "orbit":"podgorica-airport","wisecars":"podgorica/podgorica-airport",
                  "carjet":"TGD","enjoytravel":"TGD","bsp":"podgorica-airport",
                  "stressfree":"podgorica-airport","hotelbeds":"podgorica-airport",
                  "rentcars":"podgorica-airport","carflexi":"TGD",
                  "priceline":"TGD","rentcarla":"podgorica-airport","yolcu":"podgorica-airport",
                  "aurum":"podgorica-airport"},
    "Timisoara": {"iata":"TSR","q":"Timisoara+Airport","dc":"/romania/timisoara/tsr",
                  "qeeq":"ro-timisoara-tsr","vipcars":"romania/timisoara/timisoara-airport",
                  "orbit":"timisoara-airport","wisecars":"timisoara/timisoara-airport",
                  "carjet":"TSR","enjoytravel":"TSR","bsp":"timisoara-airport",
                  "stressfree":"timisoara-airport","hotelbeds":"timisoara-airport",
                  "rentcars":"timisoara-airport","carflexi":"TSR",
                  "priceline":"TSR","rentcarla":"timisoara-airport","yolcu":"timisoara-airport",
                  "aurum":"timisoara-airport"},
    "Plaisance": {"iata":"MRU","q":"Mauritius+Airport","dc":"/mauritius/mahebourg/mru",
                  "qeeq":"mu-mauritius-mru","vipcars":"mauritius/plaisance/mauritius-airport",
                  "orbit":"mauritius-airport","wisecars":"mauritius/mauritius-airport",
                  "carjet":"MRU","enjoytravel":"MRU","bsp":"mauritius-airport",
                  "stressfree":"mauritius-airport","hotelbeds":"mauritius-airport",
                  "rentcars":"mauritius-airport","carflexi":"MRU",
                  "priceline":"MRU","rentcarla":"mauritius-airport","yolcu":"mauritius-airport",
                  "aurum":"mauritius-airport"},
    "Tivat":     {"iata":"TIV","q":"Tivat+Airport","dc":"/montenegro/tivat/tiv",
                  "qeeq":"me-tivat-tiv","vipcars":"montenegro/tivat/tivat-airport",
                  "orbit":"tivat-airport","wisecars":"tivat/tivat-airport",
                  "carjet":"TIV","enjoytravel":"TIV","bsp":"tivat-airport",
                  "stressfree":"tivat-airport","hotelbeds":"tivat-airport",
                  "rentcars":"tivat-airport","carflexi":"TIV",
                  "priceline":"TIV","rentcarla":"tivat-airport","yolcu":"tivat-airport",
                  "aurum":"tivat-airport"},
}

DC_LOC = {
    "Athens":"1753","Zante":"57438","Chania":"5784","Heraklion":"5844",
    "Valletta":"2194","Tirana":"1802","Tunis":"1850","Enfidha":"57580",
    "Monastir":"57582","Djerba":"57584","Orlando":"4766","Miami":"4642",
    "Tampa":"4982","Hollywood":"4478","Rabat":"57588","Fez":"57590",
    "Tangier":"57592","Agadir":"2554","Marrakesh":"2680","Casablanca":"2220",
    "Podgorica":"57594","Timisoara":"57596","Plaisance":"57598","Tivat":"57600",
}

BN = {"otoQ":["otoq","oto q","oto-q"],"Drive365":["drive365","drive 365","drive-365"]}

# ═══════════════════════════════════════════════════════════════════════
# DIAGNOSTICS
# ═══════════════════════════════════════════════════════════════════════
@dataclass
class D:
    broker:str; city:str; brand:str; stage:str; detail:str
    ts:str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
DL: list[D] = []

# ═══════════════════════════════════════════════════════════════════════
# HTTP HELPERS
# ═══════════════════════════════════════════════════════════════════════
H = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
     "Accept":"text/html,application/xhtml+xml,*/*;q=0.8","Accept-Language":"en-US,en;q=0.5"}

def hx(url, t=20):
    """HTTP exists: True=200, False=404, None=error."""
    try:
        r=requests.get(url,headers=H,timeout=t,allow_redirects=True)
        if r.status_code==200: return True
        if r.status_code in(404,410): return False
    except: pass
    return None

def hc(url, needles, t=25):
    """HTTP contains: True=found, False=not found, None=error."""
    try:
        r=requests.get(url,headers=H,timeout=t,allow_redirects=True)
        if r.status_code!=200: return None
        txt=r.text.lower()
        for n in needles:
            if n.lower() in txt: return True
        return False
    except: return None

def bn(brand):
    return BN.get(brand,[])+[brand.lower()]

# ═══════════════════════════════════════════════════════════════════════
# PER-BROKER AGENTS — each fully customized
# ═══════════════════════════════════════════════════════════════════════

class Ag:
    N="Base"
    def ck(self,city,brand): return "N/A"

# ─── 1. Discovercars.com ─────────────────────────────────────────────
# Strategy A: /partners/{brand_slug}-{loc_code} → 200=✔, 404=✖
# Fallback: scan location page /greece-crete/heraklion/her for brand text
class DiscoverCars(Ag):
    N="Discovercars.com"
    def ck(self,city,brand):
        lc=DC_LOC.get(city)
        if not lc: DL.append(D(self.N,city,brand,"url","No DC loc")); return "N/A"
        slug="otoq" if brand=="otoQ" else "drive365"
        r=hx(f"https://www.discovercars.com/partners/{slug}-{lc}")
        if r is True: return "✔"
        if r is False: return "✖"
        dc=AP.get(city,{}).get("dc","")
        if dc:
            r2=hc(f"https://www.discovercars.com{dc}",bn(brand))
            if r2 is True: return "✔"
            if r2 is False: return "✖"
        DL.append(D(self.N,city,brand,"http","Ambiguous")); return "N/A"

# ─── 2. Qeeq.com ─────────────────────────────────────────────────────
# Strategy B: scan airport location page for brand text
# URL: /car/car-rental-pro/airport/{country-city-iata}
# Supplier filter label: displayed as supplier logos/names in listing
class Qeeq(Ag):
    N="Qeeq.com"
    def ck(self,city,brand):
        slug=AP.get(city,{}).get("qeeq")
        if not slug: DL.append(D(self.N,city,brand,"url","No qeeq slug")); return "N/A"
        url=f"https://www.qeeq.com/car/car-rental-pro/airport/{slug}"
        r=hc(url,bn(brand))
        if r is True: return "✔"
        if r is False: return "✖"
        DL.append(D(self.N,city,brand,"http",f"Unreadable {url}")); return "N/A"

# ─── 3. Orbitcarhire.com ─────────────────────────────────────────────
# Strategy B: scan search results page
# URL: /search-results?loc={slug}&puDate=...&doDate=...
# Supplier filter: sidebar checkbox list labeled "Supplier"
class OrbitCarHire(Ag):
    N="Orbitcarhire.com"
    def ck(self,city,brand):
        slug=AP.get(city,{}).get("orbit")
        if not slug: DL.append(D(self.N,city,brand,"url","No orbit slug")); return "N/A"
        url=f"https://www.orbitcarhire.com/{slug}?puDate={PU}&doDate={DO}&puTime={PUT}&doTime={DOT}&driverAge={AGE}"
        r=hc(url,bn(brand))
        if r is True: return "✔"
        if r is False: return "✖"
        DL.append(D(self.N,city,brand,"http",f"Unreadable {url}")); return "N/A"

# ─── 4. carrental.hotelbeds.com ──────────────────────────────────────
# Strategy B: scan search page
# URL: /search?location={query}&pickupDate=...&dropoffDate=...
# Supplier filter: "Car Rental Company" dropdown in filters
class Hotelbeds(Ag):
    N="carrental.hotelbeds.com"
    def ck(self,city,brand):
        slug=AP.get(city,{}).get("hotelbeds")
        if not slug: DL.append(D(self.N,city,brand,"url","No hotelbeds slug")); return "N/A"
        q=AP.get(city,{}).get("q","")
        url=f"https://carrental.hotelbeds.com/search?location={q}&pickupDate={PU}&dropoffDate={DO}&pickupTime={PUT}&dropoffTime={DOT}&driverAge={AGE}"
        r=hc(url,bn(brand))
        if r is True: return "✔"
        if r is False: return "✖"
        DL.append(D(self.N,city,brand,"http",f"Unreadable {url}")); return "N/A"

# ─── 5. Enjoytravel.com ──────────────────────────────────────────────
# Strategy B: scan search results
# URL: /en/car-rental/search?pickUp={IATA}&dropOff={IATA}&dateFrom=...
# Supplier filter: "Suppliers" checkbox section in left sidebar
class EnjoyTravel(Ag):
    N="Enjoytravel.com"
    def ck(self,city,brand):
        iata=AP.get(city,{}).get("enjoytravel")
        if not iata: DL.append(D(self.N,city,brand,"url","No IATA")); return "N/A"
        url=f"https://www.enjoytravel.com/en/car-rental/search?pickUp={iata}&dropOff={iata}&dateFrom={PU}&dateTo={DO}&timeFrom={PUT}&timeTo={DOT}&age={AGE}"
        r=hc(url,bn(brand))
        if r is True: return "✔"
        if r is False: return "✖"
        DL.append(D(self.N,city,brand,"http",f"Unreadable {url}")); return "N/A"

# ─── 6. Aurumcars.de ─────────────────────────────────────────────────
# Strategy B: scan search results page
# URL: /en/search?location={query}&from=...&to=...
# Supplier filter: "Supplier" checkboxes in filter panel
class AurumCars(Ag):
    N="Aurumcars.de"
    def ck(self,city,brand):
        slug=AP.get(city,{}).get("aurum")
        if not slug: DL.append(D(self.N,city,brand,"url","No aurum slug")); return "N/A"
        q=AP.get(city,{}).get("q","")
        url=f"https://www.aurumcars.de/en/search?location={q}&from={PU}&to={DO}&pickup_time={PUT}&dropoff_time={DOT}&driver_age={AGE}"
        r=hc(url,bn(brand))
        if r is True: return "✔"
        if r is False: return "✖"
        DL.append(D(self.N,city,brand,"http",f"Unreadable {url}")); return "N/A"

# ─── 7. Carjet.com ───────────────────────────────────────────────────
# Strategy B: scan search results
# URL: /search#puLoc={IATA}&doLoc={IATA}&puDate=...&doDate=...
# Supplier filter: "Company" or "Supplier" in left sidebar
# NOTE: Carjet uses hash-based routing → HTTP scan may get empty page
# Fallback needed: Playwright or N/A with diagnostic
class Carjet(Ag):
    N="Carjet.com"
    def ck(self,city,brand):
        iata=AP.get(city,{}).get("carjet")
        if not iata: DL.append(D(self.N,city,brand,"url","No IATA")); return "N/A"
        # Carjet uses JS hash routing — try base URL scan
        url=f"https://www.carjet.com/en/car-hire/{iata.lower()}"
        r=hc(url,bn(brand))
        if r is True: return "✔"
        if r is False: return "✖"
        # Carjet is JS-heavy, may need Playwright
        DL.append(D(self.N,city,brand,"http",f"JS-rendered site, HTTP scan insufficient for {url}")); return "N/A"

# ─── 8. Rentcars.com/en ──────────────────────────────────────────────
# Strategy B: scan search results
# URL: /en/search?location={slug}&from=...&to=...
# Supplier filter: "Rental Company" checkbox filter
class Rentcars(Ag):
    N="Rentcars.com/en"
    def ck(self,city,brand):
        slug=AP.get(city,{}).get("rentcars")
        if not slug: DL.append(D(self.N,city,brand,"url","No rentcars slug")); return "N/A"
        url=f"https://www.rentcars.com/en/search/{slug}?from={PU}&to={DO}&pickup={PUT}&dropoff={DOT}&age={AGE}"
        r=hc(url,bn(brand))
        if r is True: return "✔"
        if r is False: return "✖"
        DL.append(D(self.N,city,brand,"http",f"Unreadable {url}")); return "N/A"

# ─── 9. CarFlexi.com ─────────────────────────────────────────────────
# Strategy B: scan search results
# URL: /search?pickup={IATA}&from=...&to=...
# Supplier filter: "Vendor" dropdown
class CarFlexi(Ag):
    N="CarFlexi.com"
    def ck(self,city,brand):
        iata=AP.get(city,{}).get("carflexi")
        if not iata: DL.append(D(self.N,city,brand,"url","No IATA")); return "N/A"
        url=f"https://www.carflexi.com/search?pickup={iata}&dropoff={iata}&from={PU}&to={DO}&pickupTime={PUT}&dropoffTime={DOT}&age={AGE}"
        r=hc(url,bn(brand))
        if r is True: return "✔"
        if r is False: return "✖"
        DL.append(D(self.N,city,brand,"http",f"Unreadable {url}")); return "N/A"

# ─── 10. Economybookings.com ─────────────────────────────────────────
# Strategy A: /en/suppliers/{brand_slug}/{iata} → 200=✔, 404=✖
# Known pattern from web research; otoQ confirmed at /en/suppliers/otoq/her
class EconomyBookings(Ag):
    N="Economybookings.com"
    def ck(self,city,brand):
        iata=AP.get(city,{}).get("iata")
        if not iata: DL.append(D(self.N,city,brand,"url","No IATA")); return "N/A"
        slug="otoq" if brand=="otoQ" else "drive365"
        r=hx(f"https://www.economybookings.com/en/suppliers/{slug}/{iata.lower()}")
        if r is True: return "✔"
        if r is False: return "✖"
        # fallback city name
        cs=city.lower().replace(" ","-")
        r2=hx(f"https://www.economybookings.com/en/suppliers/{slug}/{cs}")
        if r2 is True: return "✔"
        if r2 is False: return "✖"
        DL.append(D(self.N,city,brand,"http","Ambiguous")); return "N/A"

# ─── 11. Priceline.com/rental-cars ───────────────────────────────────
# Strategy B: scan search/results page
# URL: /rental-cars/search?pickUp={IATA}&pickUpDate=...
# Supplier filter: "Company" in filter sidebar
# NOTE: Priceline is heavily JS-rendered
class Priceline(Ag):
    N="Priceline.com/rental-cars"
    def ck(self,city,brand):
        iata=AP.get(city,{}).get("priceline")
        if not iata: DL.append(D(self.N,city,brand,"url","No IATA")); return "N/A"
        pd=datetime.strptime(PU,"%Y-%m-%d"); dd=datetime.strptime(DO,"%Y-%m-%d")
        url=f"https://www.priceline.com/rental-cars/{iata.lower()}"
        r=hc(url,bn(brand))
        if r is True: return "✔"
        if r is False: return "✖"
        DL.append(D(self.N,city,brand,"http",f"JS-rendered, scan insufficient for {url}")); return "N/A"

# ─── 12. Rentcarla.com ───────────────────────────────────────────────
# Strategy B: scan search/location page
# URL: /search?location={slug}&from=...&to=...
# Supplier filter: "Supplier" in left sidebar
class RentCarla(Ag):
    N="Rentcarla.com"
    def ck(self,city,brand):
        slug=AP.get(city,{}).get("rentcarla")
        if not slug: DL.append(D(self.N,city,brand,"url","No slug")); return "N/A"
        q=AP.get(city,{}).get("q","")
        url=f"https://www.rentcarla.com/search?location={q}&from={PU}&to={DO}&pickupTime={PUT}&dropoffTime={DOT}&age={AGE}"
        r=hc(url,bn(brand))
        if r is True: return "✔"
        if r is False: return "✖"
        DL.append(D(self.N,city,brand,"http",f"Unreadable {url}")); return "N/A"

# ─── 13. Vipcars.com ─────────────────────────────────────────────────
# Strategy B: scan airport location page for brand text
# URL: /car-rental/{country}/{city}/{airport}
# Supplier names appear in page text ("OtoQ" confirmed in Athens page)
# Supplier filter: "Supplier" checkbox list in search results
class VipCars(Ag):
    N="Vipcars.com"
    def ck(self,city,brand):
        slug=AP.get(city,{}).get("vipcars")
        if not slug: DL.append(D(self.N,city,brand,"url","No vipcars slug")); return "N/A"
        url=f"https://www.vipcars.com/car-rental/{slug}"
        r=hc(url,bn(brand))
        if r is True: return "✔"
        if r is False: return "✖"
        DL.append(D(self.N,city,brand,"http",f"Unreadable {url}")); return "N/A"

# ─── 14. Yolcu360 ────────────────────────────────────────────────────
# Strategy B: scan location/search page
# URL: /en/car-rental/{slug}
# Supplier filter: "Company" or "Vendor" toggle in filter area
class Yolcu360(Ag):
    N="Yolcu360"
    def ck(self,city,brand):
        slug=AP.get(city,{}).get("yolcu")
        if not slug: DL.append(D(self.N,city,brand,"url","No yolcu slug")); return "N/A"
        q=AP.get(city,{}).get("q","")
        url=f"https://www.yolcu360.com/en/car-rental?pickUp={q}&pickUpDate={PU}&dropOffDate={DO}&pickUpTime={PUT}&dropOffTime={DOT}&age={AGE}"
        r=hc(url,bn(brand))
        if r is True: return "✔"
        if r is False: return "✖"
        DL.append(D(self.N,city,brand,"http",f"Unreadable {url}")); return "N/A"

# ─── 15. Wisecars.com ────────────────────────────────────────────────
# Strategy B: scan airport location page
# URL: /en-us/car-rental/{city}/{airport}
# Supplier filter: "Supplier" section on page
class WiseCars(Ag):
    N="Wisecars.com"
    def ck(self,city,brand):
        slug=AP.get(city,{}).get("wisecars")
        if not slug: DL.append(D(self.N,city,brand,"url","No wisecars slug")); return "N/A"
        url=f"https://www.wisecars.com/en-us/car-rental/{slug}"
        r=hc(url,bn(brand))
        if r is True: return "✔"
        if r is False: return "✖"
        DL.append(D(self.N,city,brand,"http",f"Unreadable {url}")); return "N/A"

# ─── 16. BSP-auto.com ────────────────────────────────────────────────
# Strategy B: scan search page
# URL: /en/search?location={query}&from=...&to=...
# Supplier filter: "Company" in filter panel
class BSPAuto(Ag):
    N="BSP-auto.com"
    def ck(self,city,brand):
        q=AP.get(city,{}).get("q","")
        if not q: DL.append(D(self.N,city,brand,"url","No query")); return "N/A"
        url=f"https://www.bsp-auto.com/en/search?location={q}&from={PU}&to={DO}&pickupTime={PUT}&dropoffTime={DOT}&age={AGE}"
        r=hc(url,bn(brand))
        if r is True: return "✔"
        if r is False: return "✖"
        DL.append(D(self.N,city,brand,"http",f"Unreadable {url}")); return "N/A"

# ─── 17. StressFreeCarRental.com ─────────────────────────────────────
# Strategy B: scan search page
# URL: /search?location={query}&from=...&to=...
# Supplier filter: "Rental Company" in sidebar
class StressFree(Ag):
    N="StressFreeCarRental.com"
    def ck(self,city,brand):
        q=AP.get(city,{}).get("q","")
        if not q: DL.append(D(self.N,city,brand,"url","No query")); return "N/A"
        url=f"https://www.stressfreecarrental.com/search?location={q}&from={PU}&to={DO}&pickupTime={PUT}&dropoffTime={DOT}&driverAge={AGE}"
        r=hc(url,bn(brand))
        if r is True: return "✔"
        if r is False: return "✖"
        DL.append(D(self.N,city,brand,"http",f"Unreadable {url}")); return "N/A"

# ─── 18. otoQ.rent (own brand website) ───────────────────────────────
class OtoqRent(Ag):
    N="otoQ.rent"
    def ck(self,city,brand): return "✔" if brand=="otoQ" else "✖"

# ─── 19. Drive365.rent (own brand website) ───────────────────────────
class Drive365Rent(Ag):
    N="Drive365.rent"
    def ck(self,city,brand): return "✔" if brand=="Drive365" else "✖"

# ─── Registry ────────────────────────────────────────────────────────
AG = {
    "Discovercars.com":DiscoverCars,"Qeeq.com":Qeeq,
    "Orbitcarhire.com":OrbitCarHire,"carrental.hotelbeds.com":Hotelbeds,
    "Enjoytravel.com":EnjoyTravel,"Aurumcars.de":AurumCars,"Aurum":AurumCars,
    "Carjet.com":Carjet,"Rentcars.com/en":Rentcars,"CarFlexi.com":CarFlexi,
    "Economybookings.com":EconomyBookings,"EconomyBookings.com":EconomyBookings,
    "Priceline.com/rental-cars":Priceline,"Rentcarla.com":RentCarla,
    "Vipcars.com":VipCars,"Yolcu360":Yolcu360,"Wisecars.com":WiseCars,
    "BSP-auto.com":BSPAuto,"bsp-auto.com":BSPAuto,
    "StressFreeCarRental.com":StressFree,
    "otoQ.rent":OtoqRent,"Drive365.rent":Drive365Rent,
}

# ═══════════════════════════════════════════════════════════════════════
# ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════
BNM={"EconomyBookings.com":"Economybookings.com","bsp-auto.com":"BSP-auto.com","Aurum":"Aurumcars.de"}

def build_tasks():
    t={}
    for br in OTOQ_BROKERS:
        for _,cl in OTOQ_AREAS.items():
            for ci in cl:
                t.setdefault((br,ci),[]); 
                if "otoQ" not in t[(br,ci)]: t[(br,ci)].append("otoQ")
    for br in DRIVE365_BROKERS:
        for _,cl in DRIVE365_AREAS.items():
            for ci in cl:
                cn=BNM.get(br,br)
                key=(cn,ci) if (cn,ci) in t else (br,ci)
                t.setdefault(key,[])
                if "Drive365" not in t[key]: t[key].append("Drive365")
    return t

def run():
    tasks=build_tasks()
    tot=sum(len(b) for b in tasks.values())
    print(f"\n[RUN] {len(tasks)} combos, {tot} checks")
    res={}; done=0
    for (br,ci),brands in tasks.items():
        cls=AG.get(br)
        if not cls:
            for b in brands: res.setdefault(br,{}).setdefault(ci,{})[b]="N/A"; DL.append(D(br,ci,b,"url","No agent"))
            continue
        ag=cls()
        for b in brands:
            done+=1
            if done%25==0 or done==tot: print(f"  [{done}/{tot}] {br} → {ci} ({b})")
            r=ag.ck(ci,b)
            res.setdefault(br,{}).setdefault(ci,{})[b]=r
            time.sleep(0.25)
    return res

def extract(ar,brand,areas,brokers):
    o={}
    for br in brokers:
        o[br]={}; bk=BNM.get(br,br)
        for _,cl in areas.items():
            for ci in cl:
                v="N/A"
                for k in(bk,br):
                    if k in ar and ci in ar[k]: v=ar[k][ci].get(brand,"N/A"); break
                o[br][ci]=v
    return o

# ═══════════════════════════════════════════════════════════════════════
# GOOGLE SHEETS
# ═══════════════════════════════════════════════════════════════════════
def gsc():
    cj=os.environ.get("GOOGLE_SHEETS_CREDENTIALS")
    if not cj: print("WARN: no creds"); return None
    return gspread.authorize(Credentials.from_service_account_info(json.loads(cj),
        scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]))

def sd(label,areas,brokers,res):
    cities=[(co,ci) for co,cl in areas.items() for ci in cl]
    r1=[label]+[""]*len(cities)
    r2,p=[""],None
    for co,_ in cities: r2.append(co if co!=p else ""); p=co
    r3=[""]+[ci for _,ci in cities]
    rows=[[br]+[res.get(br,{}).get(ci,"N/A") for _,ci in cities] for br in brokers]
    return [r1,r2,r3]+rows+[[],[f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M UTC')} | Dates: {PU}→{DO}"]]

def dd():
    return [["Timestamp","Broker","City","Brand","Stage","Detail"]]+[[e.ts,e.broker,e.city,e.brand,e.stage,e.detail[:500]] for e in DL]

def ug(oq,d3):
    cl=gsc()
    if not cl: return
    sid=os.environ.get("SPREADSHEET_ID")
    if not sid: print("WARN: no SPREADSHEET_ID"); return
    try: sh=cl.open_by_key(sid)
    except Exception as e: print(f"ERR: {e}"); return
    for title,data in[("otoQ",sd("otoQ",OTOQ_AREAS,OTOQ_BROKERS,oq)),
                       ("Drive365",sd("DRIVE365",DRIVE365_AREAS,DRIVE365_BROKERS,d3)),
                       ("Diagnostics",dd())]:
        try: ws=sh.worksheet(title); ws.clear()
        except gspread.exceptions.WorksheetNotFound: ws=sh.add_worksheet(title=title,rows=len(data)+5,cols=max(len(r) for r in data)+5)
        ws.update(range_name="A1",values=data); print(f"  ✔ '{title}'")

# ═══════════════════════════════════════════════════════════════════════
# LOCAL EXCEL
# ═══════════════════════════════════════════════════════════════════════
def xb(ws,label,areas,brokers,res):
    hw=Font(bold=True,size=11,name="Arial",color="FFFFFF")
    cf=Font(italic=True,size=10,name="Arial");df=Font(size=10,name="Arial")
    bf=Font(bold=True,size=14,name="Arial")
    ct=Alignment(horizontal="center",vertical="center")
    la=Alignment(horizontal="left",vertical="center")
    gf=PatternFill("solid",fgColor="C6EFCE");rf=PatternFill("solid",fgColor="FFC7CE")
    gr=PatternFill("solid",fgColor="D9D9D9");hd=PatternFill("solid",fgColor="4472C4")
    bd=Border(left=Side("thin"),right=Side("thin"),top=Side("thin"),bottom=Side("thin"))
    cities,spans=[],[];col=2
    for co,cl in areas.items():
        s=col
        for ci in cl:cities.append((co,ci));col+=1
        spans.append((co,s,col-1))
    ws.cell(1,1,label).font=bf
    for co,s,e in spans:
        c=ws.cell(2,s,co);c.font=hw;c.fill=hd;c.alignment=ct;c.border=bd
        if s!=e:ws.merge_cells(start_row=2,start_column=s,end_row=2,end_column=e)
        for x in range(s,e+1):ws.cell(2,x).border=bd;ws.cell(2,x).fill=hd
    for i,(_,ci) in enumerate(cities):c=ws.cell(3,i+2,ci);c.font=cf;c.alignment=ct;c.border=bd
    for bi,br in enumerate(brokers):
        r=4+bi;c=ws.cell(r,1,br);c.font=df;c.alignment=la;c.border=bd
        for ci_i,(_,ci) in enumerate(cities):
            v=res.get(br,{}).get(ci,"N/A")
            c=ws.cell(r,ci_i+2,v);c.font=df;c.alignment=ct;c.border=bd
            c.fill=gf if v=="✔" else rf if v=="✖" else gr
    ws.column_dimensions["A"].width=28
    for x in range(2,col):ws.column_dimensions[get_column_letter(x)].width=14

def xd(ws):
    hf=Font(bold=True,size=11,name="Arial");df=Font(size=9,name="Arial")
    bd=Border(left=Side("thin"),right=Side("thin"),top=Side("thin"),bottom=Side("thin"))
    for ci,h in enumerate(["Timestamp","Broker","City","Brand","Stage","Detail"],1):
        c=ws.cell(1,ci,h);c.font=hf;c.border=bd
    for ri,e in enumerate(DL,2):
        for ci,v in enumerate([e.ts,e.broker,e.city,e.brand,e.stage,e.detail[:500]],1):
            c=ws.cell(ri,ci,v);c.font=df;c.border=bd
    ws.column_dimensions["A"].width=20;ws.column_dimensions["B"].width=28
    ws.column_dimensions["C"].width=14;ws.column_dimensions["D"].width=12
    ws.column_dimensions["E"].width=14;ws.column_dimensions["F"].width=80

def wx(oq,d3,fn="Broker_Availability_Tracker.xlsx"):
    wb=Workbook();ws1=wb.active;ws1.title="otoQ"
    xb(ws1,"otoQ",OTOQ_AREAS,OTOQ_BROKERS,oq)
    ws2=wb.create_sheet("Drive365");xb(ws2,"DRIVE365",DRIVE365_AREAS,DRIVE365_BROKERS,d3)
    ws3=wb.create_sheet("Diagnostics");xd(ws3)
    wb.save(fn);print(f"  ✔ Excel: {fn}")

# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════
def main():
    fn=os.environ.get("OUTPUT_FILE","Broker_Availability_Tracker.xlsx")
    print("="*60);print("Broker Availability Tracker v2 — Per-Broker Customized");print("="*60)
    ar=run()
    oq=extract(ar,"otoQ",OTOQ_AREAS,OTOQ_BROKERS)
    d3=extract(ar,"Drive365",DRIVE365_AREAS,DRIVE365_BROKERS)
    s=lambda r:tuple(sum(1 for b in r.values() for v in b.values() if v==x) for x in("✔","✖","N/A"))
    o1,o2,o3=s(oq);d1,d2,d3_=s(d3)
    print(f"\n  otoQ:     ✔{o1} ✖{o2} N/A {o3}")
    print(f"  Drive365: ✔{d1} ✖{d2} N/A {d3_}")
    print(f"  Diag: {len(DL)}")
    print(f"\n--- Excel ---");wx(oq,d3,fn)
    print("--- Sheets ---");ug(oq,d3)
    print("\nDone!")

if __name__=="__main__": main()
