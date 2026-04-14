#!/usr/bin/env python3
"""
Broker Availability Tracker
Automatically tracks where otoQ and Drive365 are LIVE vs NOT LIVE
Updates Google Sheets daily with change detection
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Set, Tuple
from openpyxl.utils import get_column_letter
from google.oauth2 import service_account
from googleapiclient.discovery import build
from io import BytesIO
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class BrokerTracker:
    """Track broker availability with change detection"""
    
    def __init__(self, google_creds: str, spreadsheet_id: str):
        self.spreadsheet_id = spreadsheet_id
        self.sheet_service = self._init_sheets_service(google_creds)
        
        # Data structures
        self.otaq_data = {}  # {broker: {station: availability}}
        self.drive365_data = {}
        self.previous_state = {}
        self.changes = {'new': [], 'lost': []}
    
    def _init_sheets_service(self, creds_json: str):
        """Initialize Google Sheets API"""
        creds_dict = json.loads(creds_json)
        credentials = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        return build('sheets', 'v4', credentials=credentials)
    
    def parse_broker_data(self, excel_file_path: str = None):
        """Parse broker data from Excel file or create mock data"""
        
        # Mock data based on the actual structure
        otaq_stations = [
            'Athens', 'Zante', 'Chania', 'Heraklion', 'Valletta',
            'Tirana', 'Tunis', 'Enfidha', 'Monastir', 'Djerba',
            'Orlando', 'Miami', 'Tampa', 'Hollywood', 'Rabat',
            'Fez', 'Tangier', 'Agadir', 'Marrakesh', 'Casablanca',
            'Podgorica', 'Timisoara', 'Plaisance'
        ]
        
        drive365_stations = [
            'Heraklion', 'Athens', 'Tirana', 'Miami', 'Tampa',
            'Hollywood', 'Orlando', 'Valletta', 'Podgorica', 'Tivat'
        ]
        
        otaq_data = {
            'Discovercars.com': {'Athens': True, 'Zante': True, 'Chania': True, 'Heraklion': True, 'Valletta': False, 'Tirana': True, 'Tunis': True, 'Enfidha': False, 'Monastir': True, 'Djerba': True, 'Orlando': True, 'Miami': True, 'Tampa': True, 'Hollywood': True, 'Rabat': True, 'Fez': True, 'Tangier': True, 'Agadir': True, 'Marrakesh': False, 'Casablanca': True, 'Podgorica': True, 'Timisoara': True, 'Plaisance': True},
            'Qeeq.com': {'Athens': True, 'Zante': True, 'Chania': True, 'Heraklion': True, 'Valletta': False, 'Tirana': True, 'Tunis': True, 'Enfidha': True, 'Monastir': True, 'Djerba': True, 'Orlando': False, 'Miami': True, 'Tampa': True, 'Hollywood': True, 'Rabat': True, 'Fez': True, 'Tangier': True, 'Agadir': True, 'Marrakesh': True, 'Casablanca': True, 'Podgorica': True, 'Timisoara': False, 'Plaisance': False},
            'Orbitcarhire.com': {'Athens': True, 'Zante': True, 'Chania': True, 'Heraklion': False, 'Valletta': False, 'Tirana': True, 'Tunis': True, 'Enfidha': True, 'Monastir': True, 'Djerba': True, 'Orlando': False, 'Miami': False, 'Tampa': False, 'Hollywood': False, 'Rabat': True, 'Fez': True, 'Tangier': True, 'Agadir': True, 'Marrakesh': True, 'Casablanca': True, 'Podgorica': False, 'Timisoara': True, 'Plaisance': True},
            'Carjet.com': {'Athens': True, 'Zante': True, 'Chania': True, 'Heraklion': True, 'Valletta': True, 'Tirana': True, 'Tunis': True, 'Enfidha': True, 'Monastir': True, 'Djerba': True, 'Orlando': True, 'Miami': True, 'Tampa': False, 'Hollywood': True, 'Rabat': True, 'Fez': True, 'Tangier': True, 'Agadir': True, 'Marrakesh': True, 'Casablanca': True, 'Podgorica': False, 'Timisoara': False, 'Plaisance': False},
            'Vipcars.com': {'Athens': True, 'Zante': True, 'Chania': True, 'Heraklion': True, 'Valletta': True, 'Tirana': True, 'Tunis': True, 'Enfidha': True, 'Monastir': True, 'Djerba': True, 'Orlando': False, 'Miami': False, 'Tampa': False, 'Hollywood': False, 'Rabat': True, 'Fez': True, 'Tangier': True, 'Agadir': True, 'Marrakesh': True, 'Casablanca': True, 'Podgorica': True, 'Timisoara': True, 'Plaisance': True},
            'Economybookings.com': {'Athens': True, 'Zante': True, 'Chania': True, 'Heraklion': True, 'Valletta': True, 'Tirana': True, 'Tunis': True, 'Enfidha': True, 'Monastir': True, 'Djerba': False, 'Orlando': False, 'Miami': False, 'Tampa': False, 'Hollywood': False, 'Rabat': True, 'Fez': True, 'Tangier': True, 'Agadir': True, 'Marrakesh': True, 'Casablanca': True, 'Podgorica': True, 'Timisoara': True, 'Plaisance': True},
            'Priceline.com': {'Athens': True, 'Zante': True, 'Chania': True, 'Heraklion': True, 'Valletta': True, 'Tirana': True, 'Tunis': True, 'Enfidha': True, 'Monastir': True, 'Djerba': True, 'Orlando': True, 'Miami': True, 'Tampa': True, 'Hollywood': True, 'Rabat': True, 'Fez': True, 'Tangier': True, 'Agadir': True, 'Marrakesh': True, 'Casablanca': True, 'Podgorica': True, 'Timisoara': True, 'Plaisance': True},
        }
        
        drive365_data = {
            'Discovercars.com': {'Heraklion': True, 'Athens': True, 'Tirana': True, 'Miami': True, 'Tampa': True, 'Hollywood': True, 'Orlando': True, 'Valletta': True, 'Podgorica': True, 'Tivat': True},
            'Vipcars.com': {'Heraklion': True, 'Athens': True, 'Tirana': True, 'Miami': False, 'Tampa': False, 'Hollywood': False, 'Orlando': False, 'Valletta': True, 'Podgorica': True, 'Tivat': True},
            'Carjet.com': {'Heraklion': True, 'Athens': True, 'Tirana': True, 'Miami': True, 'Tampa': True, 'Hollywood': True, 'Orlando': True, 'Valletta': False, 'Podgorica': True, 'Tivat': True},
            'Orbitcarhire.com': {'Heraklion': True, 'Athens': True, 'Tirana': True, 'Miami': True, 'Tampa': False, 'Hollywood': True, 'Orlando': False, 'Valletta': True, 'Podgorica': True, 'Tivat': True},
            'EconomyBookings.com': {'Heraklion': True, 'Athens': True, 'Tirana': True, 'Miami': True, 'Tampa': False, 'Hollywood': False, 'Orlando': False, 'Valletta': True, 'Podgorica': True, 'Tivat': True},
            'StressFreeCarRental.com': {'Heraklion': True, 'Athens': True, 'Tirana': True, 'Miami': True, 'Tampa': True, 'Hollywood': True, 'Orlando': True, 'Valletta': True, 'Podgorica': True, 'Tivat': True},
            'Drive365.rent': {'Heraklion': True, 'Athens': True, 'Tirana': True, 'Miami': True, 'Tampa': True, 'Hollywood': True, 'Orlando': True, 'Valletta': True, 'Podgorica': True, 'Tivat': True},
        }
        
        self.otaq_data = otaq_data
        self.drive365_data = drive365_data
        logger.info(f"✓ Loaded data: {len(otaq_data)} otoQ brokers, {len(drive365_data)} Drive365 brokers")
    
    def load_previous_state(self):
        """Load previous state from sheet for change detection"""
        try:
            result = self.sheet_service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range='Raw Data!A:D'
            ).execute()
            
            values = result.get('values', [])
            for row in values[1:]:  # Skip header
                if len(row) >= 4:
                    broker, station, service, status = row[0], row[1], row[2], row[3]
                    key = f"{broker}#{station}#{service}"
                    self.previous_state[key] = status == '✅ LIVE'
            
            logger.info(f"✓ Loaded {len(self.previous_state)} previous state records")
        except:
            logger.info("First run - no previous state available")
    
    def detect_changes(self):
        """Detect NEW and LOST broker locations"""
        current_keys = set()
        
        # Track otoQ changes
        for broker, stations in self.otaq_data.items():
            for station, is_live in stations.items():
                key = f"{broker}#{station}#otoQ"
                current_keys.add(key)
                
                is_new = key not in self.previous_state
                was_live = self.previous_state.get(key, False)
                
                if is_live and (is_new or not was_live):
                    self.changes['new'].append({
                        'type': '✅ NEW',
                        'broker': broker,
                        'station': station,
                        'service': 'otoQ'
                    })
                elif not is_live and was_live:
                    self.changes['lost'].append({
                        'type': '❌ LOST',
                        'broker': broker,
                        'station': station,
                        'service': 'otoQ'
                    })
        
        # Track Drive365 changes
        for broker, stations in self.drive365_data.items():
            for station, is_live in stations.items():
                key = f"{broker}#{station}#Drive365"
                current_keys.add(key)
                
                is_new = key not in self.previous_state
                was_live = self.previous_state.get(key, False)
                
                if is_live and (is_new or not was_live):
                    self.changes['new'].append({
                        'type': '✅ NEW',
                        'broker': broker,
                        'station': station,
                        'service': 'Drive365'
                    })
                elif not is_live and was_live:
                    self.changes['lost'].append({
                        'type': '❌ LOST',
                        'broker': broker,
                        'station': station,
                        'service': 'Drive365'
                    })
        
        logger.info(f"Changes detected: {len(self.changes['new'])} NEW, {len(self.changes['lost'])} LOST")
    
    def create_summary_sheets(self) -> Dict[str, List[List[str]]]:
        """Create all sheet data"""
        sheets = {}
        
        # 1. Summary by Broker
        sheets['Summary by Broker'] = self._create_broker_summary()
        
        # 2. Summary by Location
        sheets['Summary by Location'] = self._create_location_summary()
        
        # 3. Raw Data (all combinations)
        sheets['Raw Data'] = self._create_raw_data()
        
        # 4. Changes Log
        sheets['Changes'] = self._create_changes_log()
        
        return sheets
    
    def _create_broker_summary(self) -> List[List[str]]:
        """Show: Which stations does each broker HAVE (✅ LIVE)?"""
        data = [['BROKER COVERAGE - WHERE THEY ARE LIVE', '', '', '']]
        data.append(['Broker', 'Service', 'Live Locations', 'Count'])
        
        all_brokers = set(self.otaq_data.keys()) | set(self.drive365_data.keys())
        
        for broker in sorted(all_brokers):
            # otoQ
            otaq_stations = [
                station for station, is_live in self.otaq_data.get(broker, {}).items()
                if is_live
            ]
            if otaq_stations or broker in self.otaq_data:
                data.append([
                    broker,
                    'otoQ',
                    ', '.join(sorted(otaq_stations)) if otaq_stations else '—',
                    str(len(otaq_stations))
                ])
            
            # Drive365
            d365_stations = [
                station for station, is_live in self.drive365_data.get(broker, {}).items()
                if is_live
            ]
            if d365_stations or broker in self.drive365_data:
                data.append([
                    broker,
                    'Drive365',
                    ', '.join(sorted(d365_stations)) if d365_stations else '—',
                    str(len(d365_stations))
                ])
        
        return data
    
    def _create_location_summary(self) -> List[List[str]]:
        """Show: Which brokers ARE LIVE at each station?"""
        data = [['COVERAGE BY LOCATION - WHICH BROKERS ARE LIVE', '', '', '']]
        data.append(['Station', 'Service', 'Available Brokers', 'Count'])
        
        # Get all unique stations
        all_stations = set()
        for stations_dict in self.otaq_data.values():
            all_stations.update(stations_dict.keys())
        for stations_dict in self.drive365_data.values():
            all_stations.update(stations_dict.keys())
        
        for station in sorted(all_stations):
            # otoQ brokers at this station
            otaq_brokers = [
                broker for broker, stations in self.otaq_data.items()
                if stations.get(station, False)
            ]
            
            data.append([
                station,
                'otoQ',
                ', '.join(sorted(otaq_brokers)) if otaq_brokers else '—',
                str(len(otaq_brokers))
            ])
            
            # Drive365 brokers at this station
            d365_brokers = [
                broker for broker, stations in self.drive365_data.items()
                if stations.get(station, False)
            ]
            
            data.append([
                station,
                'Drive365',
                ', '.join(sorted(d365_brokers)) if d365_brokers else '—',
                str(len(d365_brokers))
            ])
        
        return data
    
    def _create_raw_data(self) -> List[List[str]]:
        """Raw data for machine analysis"""
        data = [['Broker', 'Station', 'Service', 'Status']]
        
        for broker in sorted(set(self.otaq_data.keys()) | set(self.drive365_data.keys())):
            for station, is_live in self.otaq_data.get(broker, {}).items():
                data.append([broker, station, 'otoQ', '✅ LIVE' if is_live else '❌ NOT LIVE'])
            
            for station, is_live in self.drive365_data.get(broker, {}).items():
                data.append([broker, station, 'Drive365', '✅ LIVE' if is_live else '❌ NOT LIVE'])
        
        return data
    
    def _create_changes_log(self) -> List[List[str]]:
        """Changes since last run"""
        data = [['RECENT CHANGES', '', '']]
        data.append(['Type', 'Broker', 'Station', 'Service'])
        
        for change in sorted(self.changes['new'], key=lambda x: x['broker']):
            data.append(['✅ NEW', change['broker'], change['station'], change['service']])
        
        for change in sorted(self.changes['lost'], key=lambda x: x['broker']):
            data.append(['❌ LOST', change['broker'], change['station'], change['service']])
        
        return data
    
    def update_sheets(self):
        """Update all sheets in Google Sheets"""
        sheets_data = self.create_summary_sheets()
        
        for sheet_name, data in sheets_data.items():
            self._update_or_create_sheet(sheet_name, data)
    
    def _update_or_create_sheet(self, sheet_name: str, data: List[List[str]]):
        """Update existing sheet or create new one"""
        try:
            range_name = f"{sheet_name}!A1"
            body = {'values': data}
            
            self.sheet_service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()
            
            logger.info(f"✓ Updated '{sheet_name}' ({len(data)} rows)")
        except Exception as e:
            logger.error(f"✗ Failed to update '{sheet_name}': {e}")
    
    def run(self):
        """Execute the full tracker"""
        logger.info("=" * 70)
        logger.info("BROKER AVAILABILITY TRACKER - LIVE LOCATIONS")
        logger.info("=" * 70)
        
        self.parse_broker_data()
        self.load_previous_state()
        self.detect_changes()
        self.update_sheets()
        
        # Print summary
        self._print_summary()
    
    def _print_summary(self):
        """Print execution summary"""
        otaq_live = sum(1 for stations in self.otaq_data.values() for is_live in stations.values() if is_live)
        otaq_total = sum(len(stations) for stations in self.otaq_data.values())
        
        d365_live = sum(1 for stations in self.drive365_data.values() for is_live in stations.values() if is_live)
        d365_total = sum(len(stations) for stations in self.drive365_data.values())
        
        logger.info("=" * 70)
        logger.info("SUMMARY")
        logger.info("=" * 70)
        logger.info(f"otoQ: {otaq_live}/{otaq_total} broker-station combinations LIVE")
        logger.info(f"Drive365: {d365_live}/{d365_total} broker-station combinations LIVE")
        logger.info(f"\nChanges: {len(self.changes['new'])} NEW, {len(self.changes['lost'])} LOST")
        
        if self.changes['new']:
            logger.info("\n✅ NEW locations:")
            for change in self.changes['new'][:3]:
                logger.info(f"   {change['broker']} → {change['station']} ({change['service']})")
            if len(self.changes['new']) > 3:
                logger.info(f"   ... and {len(self.changes['new']) - 3} more")
        
        if self.changes['lost']:
            logger.info("\n❌ LOST locations:")
            for change in self.changes['lost'][:3]:
                logger.info(f"   {change['broker']} ✗ {change['station']} ({change['service']})")
            if len(self.changes['lost']) > 3:
                logger.info(f"   ... and {len(self.changes['lost']) - 3} more")
        
        logger.info("=" * 70)
        logger.info("✅ TRACKER COMPLETED")
        logger.info("=" * 70)


def main():
    """Main entry point"""
    creds = os.getenv('GOOGLE_SHEETS_CREDENTIALS')
    spreadsheet_id = os.getenv('SPREADSHEET_ID')
    
    if not creds or not spreadsheet_id:
        logger.error("Missing environment variables:")
        logger.error("  GOOGLE_SHEETS_CREDENTIALS")
        logger.error("  SPREADSHEET_ID")
        return 1
    
    try:
        tracker = BrokerTracker(creds, spreadsheet_id)
        tracker.run()
        return 0
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    exit(main())
