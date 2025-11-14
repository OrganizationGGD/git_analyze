import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Any
import multiprocessing as mp
import warnings
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import time
import cachetools

warnings.filterwarnings('ignore')


class GeoLocationService:
    def __init__(self, cache_size: int = 1000):
        self.geolocator = Nominatim(user_agent="contributor_analysis")
        self.cache = cachetools.LRUCache(maxsize=cache_size)
        self.last_request_time = 0

    def geocode_location(self, location_str: str) -> Optional[Dict]:
        if not location_str or location_str.lower() in ['', 'unknown', 'none', 'null']:
            return None

        cache_key = location_str.lower().strip()
        if cache_key in self.cache:
            return self.cache[cache_key]

        try:
            location = self.geolocator.geocode(
                location_str,
                addressdetails=True,
                language='en',
                timeout=60,
            )

            if location:
                result = {
                    'latitude': location.latitude,
                    'longitude': location.longitude,
                    'address': location.raw.get('address', {}),
                    'raw': location.raw
                }
                self.cache[cache_key] = result
                return result

        except (GeocoderTimedOut, GeocoderServiceError) as e:
            print(f"Geocoding error for '{location_str}': {e}")
        except Exception as e:
            print(f"Unexpected error for '{location_str}': {e}")

        return None

    def extract_country_from_geodata(self, geodata: Dict) -> str:
        if not geodata:
            return None

        address = geodata.get('address', {})
        country = address.get('country', '')

        if country:
            return country.lower()

        return None

    def extract_city_from_geodata(self, geodata: Dict) -> str:
        if not geodata:
            return None

        address = geodata.get('address', {})

        city_fields = ['city', 'town', 'village', 'municipality']
        for field in city_fields:
            if field in address and address[field]:
                return address[field].lower()

        return None


class ContributorLocationClustering:
    def __init__(self, location_repo, n_workers: int = None):
        self.location_repo = location_repo
        self.n_workers = n_workers or mp.cpu_count()
        self.geo_service = GeoLocationService()

    def _process_and_save_single_contributor(self, row) -> bool:
        contributor_id = row['contributor_id']
        original_location = str(row['location'] or '')

        if not original_location or original_location.strip() == '':
            return False

        geodata = self.geo_service.geocode_location(original_location)

        if not geodata:
            return False

        country = self.geo_service.extract_country_from_geodata(geodata)
        city = self.geo_service.extract_city_from_geodata(geodata)

        if not country:
            return False

        try:
            success = self.location_repo.save_contributor_location(
                contributor_id=contributor_id,
                original_location=original_location,
                country_name=country,
                city_name=city,
                latitude=geodata.get('latitude'),
                longitude=geodata.get('longitude')
            )
            if success:
                print(f"✓ Saved location for contributor {contributor_id}: {country}, {city}")
            return success
        except Exception as e:
            print(f"✗ Error saving location for contributor {contributor_id}: {e}")
            return False

    def process_contributors(self, df: pd.DataFrame) -> int:
        print("Processing contributor locations...")

        total = len(df)

        with ThreadPoolExecutor(max_workers=self.n_workers) as executor:
            results = list(executor.map(self._process_and_save_single_contributor, [row for _, row in df.iterrows()]))
            successful = sum(results)

        print(f"Completed: {successful}/{total} locations determined")
        return successful

    def run_location_analysis(self) -> Dict[str, Any]:
        print("Starting contributor location analysis...")

        try:
            df = self.location_repo.load_contributor_location_data()

            if df.empty:
                return {"error": "No contributor data available"}

            print(f"Loaded {len(df)} contributors")

            successful = self.process_contributors(df)

            return {
                "successful_locations": successful,
                "total_contributors": len(df)
            }

        except Exception as e:
            print(f"Error during location analysis: {e}")
            return {"error": str(e)}


class LocationAnalyzer:
    def __init__(self, database_url: str, n_workers: int = None):
        from src.storage.unit_of_work import UnitOfWork
        from src.analysis.clustering.place.repo.repo import LocationRepository

        UnitOfWork(database_url).create_location_tables()
        location_repo = LocationRepository(database_url)
        self.analyzer = ContributorLocationClustering(location_repo, n_workers)

    def analyze(self) -> Dict:
        return self.analyzer.run_location_analysis()