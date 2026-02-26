"""
Enhanced API Client for external data sources
Supports: Sensor Tower, Facebook Ads, TikTok Ads with production-ready features
Features: Rate limiting, exponential backoff, batch processing, parallel execution
"""

import itertools
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from airflow.models import Variable

logger = logging.getLogger(__name__)


class APIClient:
    """Generic API client with rate limiting and retry logic"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize API client with configuration
        
        Args:
            config: Dict containing:
                - api_name: str (e.g., 'sensortower', 'facebook', 'tiktok')
                - base_url: str
                - token: str or Dict (for different auth types)
                - headers: Optional[Dict]
                - timeout: Optional[int] (default: 30)
                - max_retries: Optional[int] (default: 3)
                - request_delay: Optional[float] (default: 0.5)
                - batch_delay: Optional[float] (default: 2.0)
                - initial_retry_delay: Optional[float] (default: 1.0)
                - retry_backoff: Optional[int] (default: 2)
        """
        self.api_name = config.get('api_name', 'unknown')
        self.base_url = config['base_url'].rstrip('/')
        self.token = config['token']
        self.headers = config.get('headers', {})
        self.timeout = config.get('timeout', 30)
        self.max_retries = config.get('max_retries', 3)
        self.request_delay = config.get('request_delay', 0.5)  # Rate limiting
        self.batch_delay = config.get('batch_delay', 2.0)
        self.initial_retry_delay = config.get('initial_retry_delay', 1.0)
        self.retry_backoff = config.get('retry_backoff', 2)
        
        # Reuse HTTP session for better performance
        self.session = requests.Session()
        
        # Setup authentication
        self._setup_auth()
        
    def _setup_auth(self):
        """Setup authentication headers based on token type"""
        if isinstance(self.token, str):
            # Bearer token or API key
            if 'authorization' not in [k.lower() for k in self.headers.keys()]:
                self.headers['Authorization'] = f'Bearer {self.token}'
        elif isinstance(self.token, dict):
            # Custom auth (e.g., OAuth, custom headers)
            self.headers.update(self.token)
    
    def _make_request_with_retry(self, url: str, method: str = 'GET', 
                                  params: Optional[Dict] = None, 
                                  data: Optional[Dict] = None,
                                  json_data: Optional[Dict] = None) -> Optional[Dict]:
        """
        Make HTTP request with exponential backoff retry logic for 429 errors
        
        Args:
            url: Full URL to request
            method: HTTP method (GET or POST)
            params: Query parameters
            data: Form data
            json_data: JSON body
            
        Returns:
            Response JSON as dict or None if failed
        """
        retry_delay = self.initial_retry_delay
        
        for attempt in range(self.max_retries + 1):
            try:
                # Rate limiting between requests
                time.sleep(self.request_delay)
                
                if method == 'GET':
                    response = self.session.get(url, headers=self.headers, params=params, timeout=self.timeout)
                else:
                    response = self.session.post(url, headers=self.headers, params=params, 
                                                data=data, json=json_data, timeout=self.timeout)
                
                # Handle 429 Too Many Requests
                if response.status_code == 429:
                    if attempt < self.max_retries:
                        print(f"⚠️ Rate limited (429). Retrying in {retry_delay}s (attempt {attempt + 1}/{self.max_retries})...", flush=True)
                        time.sleep(retry_delay)
                        retry_delay *= self.retry_backoff
                        continue
                    else:
                        print(f"🔥 Rate limited after {self.max_retries} retries. Giving up.", flush=True)
                        raise requests.exceptions.HTTPError(f"HTTP 429 Too Many Requests after {self.max_retries} retries")
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries and (not hasattr(response, 'status_code') or response.status_code != 429):
                    print(f"⚠️ Request failed: {e}. Retrying in {retry_delay}s...", flush=True)
                    time.sleep(retry_delay)
                    retry_delay *= self.retry_backoff
                    continue
                raise
        
        raise requests.exceptions.RequestException("Max retries exceeded")
    
    def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Make GET request to API
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            Response JSON as dict
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.info(f"[{self.api_name}] GET {url}")
        return self._make_request_with_retry(url, method='GET', params=params)
    
    def post(self, endpoint: str, data: Optional[Dict] = None, json_data: Optional[Dict] = None) -> Dict:
        """
        Make POST request to API
        
        Args:
            endpoint: API endpoint path
            data: Form data
            json_data: JSON body
            
        Returns:
            Response JSON as dict
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.info(f"[{self.api_name}] POST {url}")
        return self._make_request_with_retry(url, method='POST', data=data, json_data=json_data)
    
    @staticmethod
    def chunks(iterable, n):
        """Yields successive n-sized chunks from any iterable"""
        it = iter(iterable)
        while True:
            chunk_it = itertools.islice(it, n)
            try:
                first_el = next(chunk_it)
            except StopIteration:
                return
            yield itertools.chain([first_el], chunk_it)


class SensorTowerClient(APIClient):
    """
    SensorTower API client with production-ready features
    Based on proven patterns from new_games and top_games scripts
    """
    
    def __init__(self, token: str):
        config = {
            'api_name': 'sensortower',
            'base_url': 'https://api.sensortower.com/v1',
            'token': token,
            'headers': {},
            'max_retries': 3,
            'request_delay': 0.5,  # 500ms between requests
            'batch_delay': 2.0,     # 2s between batches
            'initial_retry_delay': 1.0,
            'retry_backoff': 2
        }
        super().__init__(config)
        self.token = Variable.get("SENSOR_TOWER_API_TOKEN")  # Store for URL params
    
    def get_new_apps(self, os_platform: str, category: str, start_date: str, 
                     limit: int = 10000) -> List[str]:
        """
        Fetch all new app IDs for a given platform, category, and start date
        Handles pagination automatically
        
        Args:
            os_platform: 'ios' or 'android'
            category: '6014' for iOS games, 'game' for Android games
            start_date: Date in YYYY-MM-DD format
            limit: Results per page (default: 10000)
            
        Returns:
            List of app IDs
        """
        all_apps = []
        offset = 10000
        
        print(f'---- GETTING NEW {os_platform.upper()} APPS for category: {category} ----', flush=True)
        
        while True:
            params = {
                'category': category,
                'start_date': start_date,
                'limit': limit,
                'offset': offset,
                'auth_token': self.token
            }
            
            print(f"  Fetching page with offset={offset}...", flush=True)
            
            try:
                url = f"{self.base_url}/{os_platform}/apps/app_ids"
                data = self._make_request_with_retry(url, params=params)
                
                if not data or "ids" not in data:
                    print(f"⚠️ Unexpected API response: {data}", flush=True)
                    break
                
                app_ids = data["ids"]
                
                if not app_ids:
                    print("  No more apps found. Pagination complete.", flush=True)
                    break
                
                all_apps.extend(app_ids)
                offset += limit
                time.sleep(self.batch_delay)  # Delay between pagination requests
                
            except requests.exceptions.RequestException as e:
                print(f"🔥 ERROR while fetching new apps: {e}", flush=True)
                break
            except Exception as e:
                print(f"🔥 UNEXPECTED ERROR while fetching new apps: {e}", flush=True)
                break
        
        print(f"---- Found a total of {len(all_apps)} new apps. ----", flush=True)
        return all_apps
    
    def _process_metadata_batch(self, batch_info: Tuple) -> Optional[pd.DataFrame]:
        """
        Fetch and process metadata and game tags for a single batch of app IDs
        
        Args:
            batch_info: Tuple of (batch_num, batch_ids, os_platform)
            
        Returns:
            DataFrame with merged metadata and tags or None if failed
        """
        batch_num, batch_ids, os_platform = batch_info
        ids_str = ",".join(map(str, batch_ids))
        
        print(f"▶️ Starting Metadata Batch {batch_num} ({len(batch_ids)} apps)...", flush=True)
        
        try:
            # Step 1: Fetch metadata
            print(f"  Batch {batch_num}: Fetching metadata...", flush=True)
            metadata_params = {
                'app_ids': ids_str,
                'country': 'US',
                'include_sdk_data': 'false',
                'auth_token': self.token
            }
            metadata_url = f"{self.base_url}/{os_platform}/apps"
            metadata = self._make_request_with_retry(metadata_url, params=metadata_params)
            
            if not metadata or not metadata.get("apps"):
                print(f"🟡 Batch {batch_num}: No metadata found. Skipping.", flush=True)
                return None
            
            metadata_df = pd.DataFrame(metadata["apps"])
            
            # Step 2: Fetch game tags
            print(f"  Batch {batch_num}: Fetching game tags...", flush=True)
            time.sleep(self.request_delay)
            
            game_info_params = {
                'app_ids': ids_str,
                'fields[]': [
                    'Game Art Style',
                    'Game Genre',
                    'Game Sub-genre',
                    'Game Theme',
                    'Free',
                    'IP: Corporate Parent',
                    'Revenue First 30 Days (WW)'
                ],
                'auth_token': self.token
            }
            game_info_url = f"{self.base_url}/app_tag/tags_for_apps"
            game_info = self._make_request_with_retry(game_info_url, params=game_info_params)
            
            # Step 3: Merge metadata and game info
            if game_info and game_info.get("data"):
                rows = [
                    {"app_id": entry["app_id"], **{tag["name"]: tag["exact_value"] for tag in entry["tags"]}}
                    for entry in game_info["data"]
                ]
                if rows:
                    game_info_df = pd.DataFrame(rows)
                    merged_df = metadata_df.merge(game_info_df, on="app_id", how="left")
                    print(f"✅ Batch {batch_num}: Success. Merged metadata and tags.", flush=True)
                    return merged_df
            
            # If no game info, return just metadata
            print(f"✅ Batch {batch_num}: Success (metadata only).", flush=True)
            return metadata_df
            
        except requests.exceptions.RequestException as e:
            print(f"🔥 NETWORK ERROR in Metadata Batch {batch_num}: {e}", flush=True)
            return None
        except Exception as e:
            print(f"🔥 UNEXPECTED ERROR in Metadata Batch {batch_num}: {e}", flush=True)
            return None
    
    def get_metadata_parallel(self, app_ids: List[str], os_platform: str = 'android', 
                             batch_size: int = 100, max_workers: int = 3) -> pd.DataFrame:
        """
        Fetch app metadata in parallel batches
        
        Args:
            app_ids: List of app IDs to fetch
            os_platform: 'ios' or 'android'
            batch_size: Number of apps per batch (default: 100)
            max_workers: Number of parallel workers (default: 3)
            
        Returns:
            DataFrame with all metadata
        """
        print(f'---- STARTING PARALLEL FETCH for {os_platform.upper()} apps with {max_workers} workers ----', flush=True)
        
        if not app_ids:
            print("Input 'app_ids' is empty. No data to fetch.", flush=True)
            return pd.DataFrame()
        
        batch_generator = self.chunks(app_ids, batch_size)
        tasks = [(i, list(batch), os_platform) for i, batch in enumerate(batch_generator, 1)]
        
        if not tasks:
            return pd.DataFrame()
        
        all_results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = executor.map(self._process_metadata_batch, tasks)
            all_results = [res for res in results if res is not None]
            time.sleep(self.batch_delay)
        
        if not all_results:
            print("\n---- No data was successfully fetched for any batch. ----", flush=True)
            return pd.DataFrame()
        
        print("\n---- All batches processed. Concatenating final DataFrame. ----", flush=True)
        return pd.concat(all_results, ignore_index=True)
    
    def _process_performance_batch(self, batch_info: Tuple) -> Optional[pd.DataFrame]:
        """
        Fetch and process monthly performance for a single batch of app IDs
        
        Args:
            batch_info: Tuple of (batch_num, batch_ids, os_platform, countries, start_date, end_date)
            
        Returns:
            DataFrame with performance data or None if failed
        """
        batch_num, batch_ids, os_platform, countries, start_date, end_date = batch_info
        ids_str = ",".join(map(str, batch_ids))
        countries_str = ",".join(countries)
        
        print(f"▶️ Starting Performance Batch {batch_num} ({len(batch_ids)} apps)...", flush=True)
        
        try:
            params = {
                'app_ids': ids_str,
                'countries': countries_str,
                'date_granularity': 'monthly',
                'start_date': start_date,
                'end_date': end_date,
                'auth_token': self.token
            }
            
            url = f"{self.base_url}/{os_platform}/sales_report_estimates"
            data = self._make_request_with_retry(url, params=params)
            
            if not data:
                print(f"🟡 Batch {batch_num}: No performance data found. Skipping.", flush=True)
                return None
            
            print(f"✅ Batch {batch_num}: Success fetching performance data.", flush=True)
            return pd.DataFrame(data)
            
        except requests.exceptions.RequestException as e:
            print(f"🔥 NETWORK ERROR in Performance Batch {batch_num}: {e}", flush=True)
            return None
        except Exception as e:
            print(f"🔥 UNEXPECTED ERROR in Performance Batch {batch_num}: {e}", flush=True)
            return None
    
    def get_monthly_performance_parallel(self, app_ids: List[str], countries: List[str], 
                                        start_date: str, end_date: str, os_platform: str = 'android',
                                        batch_size: int = 100, max_workers: int = 3) -> pd.DataFrame:
        """
        Fetch monthly performance data in parallel batches
        
        Args:
            app_ids: List of app IDs to fetch
            countries: List of country codes
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            os_platform: 'ios' or 'android'
            batch_size: Number of apps per batch (default: 100)
            max_workers: Number of parallel workers (default: 3)
            
        Returns:
            DataFrame with performance data
        """
        print(f'---- STARTING PARALLEL PERFORMANCE FETCH for {os_platform.upper()} ----', flush=True)
        
        if not app_ids:
            return pd.DataFrame()
        
        batch_generator = self.chunks(app_ids, batch_size)
        tasks = [(i, list(batch), os_platform, countries, start_date, end_date) 
                for i, batch in enumerate(batch_generator, 1)]
        
        if not tasks:
            return pd.DataFrame()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = executor.map(self._process_performance_batch, tasks)
            all_results = [res for res in results if res is not None]
            time.sleep(self.batch_delay)
        
        if not all_results:
            return pd.DataFrame()
        
        return pd.concat(all_results, ignore_index=True)
    
    def get_top_apps(self, start_date: str, end_date: str, 
                    category: str = '6014', measure: str = 'revenue',
                    device_type: str = 'total', limit: int = 1000) -> pd.DataFrame:
        """
        Fetch top apps for a given country using unified sales report API
        
        Args:
            country: Country code (e.g., 'VN', 'TH', 'US')
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            category: '6014' for iOS games, 'game' for Android
            measure: 'revenue' or 'downloads'
            device_type: 'total', 'iphone', 'ipad', etc.
            limit: Number of results (default: 1000)
            
        Returns:
            DataFrame with top apps data
        """
        print(f'---- DISCOVERING TOP APPS for {country} ----', flush=True)
        
        params = {
            'comparison_attribute': 'absolute',
            'time_range': 'month',
            'measure': measure,
            'device_type': device_type,
            'category': category,
            'date': start_date,
            'end_date': end_date,
            'limit': limit,
            'custom_tags_mode': 'include_unified_apps',
            'data_model': 'DM_2025_Q2',
            'auth_token': self.token,
            'regions': country
        }
        
        print(f"  Fetching data for {country}...", flush=True)
        
        url = f"{self.base_url}/unified/sales_report_estimates_comparison_attributes"
        data = self._make_request_with_retry(url, params=params)
        
        if not data or not isinstance(data, list) or not data:
            print(f"  No data found for {country}")
            return pd.DataFrame()
        
        # Process data
        records = self._process_top_apps_data(data)
        
        if records:
            df = pd.DataFrame(records)
            print(f"---- Discovered {len(records)} apps for {country} ----", flush=True)
            return df
        else:
            print(f"  No valid records found for {country}")
            return pd.DataFrame()
    
    def _process_top_apps_data(self, data: List[Dict]) -> List[Dict]:
        """Process top apps API response data"""
        import re
        records = []
        
        for outer in data:
            entities = outer.get("entities", [])
            if not entities:
                continue
            
            for entity in entities:
                try:
                    app_id = entity.get("app_id")
                    if not app_id:
                        continue
                    
                    row = {
                        "app_id": app_id,
                        "country": entity.get("country", ""),
                        "month": entity.get("date", "")[:10],
                        "downloads": entity.get("units_absolute", 0),
                        "revenue": entity.get("revenue_absolute", 0) / 100
                    }
                    
                    # Process custom tags
                    tags = entity.get("custom_tags", {})
                    row["Free"] = tags.get("Free")
                    row["Game Art Style"] = tags.get("Game Art Style")
                    row["Game Genre"] = tags.get("Game Genre")
                    row["Game Sub-genre"] = tags.get("Game Sub-genre")
                    row["Game Theme"] = tags.get("Game Theme")
                    
                    # Process revenue with error handling
                    rev30_str = tags.get("Revenue First 30 Days (WW)")
                    if isinstance(rev30_str, str) and rev30_str:
                        try:
                            clean_rev = re.sub(r"[^0-9]", "", rev30_str)
                            row["Revenue First 30 Days (WW)"] = int(clean_rev) if clean_rev else 0
                        except (ValueError, TypeError):
                            row["Revenue First 30 Days (WW)"] = 0
                    else:
                        row["Revenue First 30 Days (WW)"] = None
                    
                    records.append(row)
                    
                except Exception as e:
                    print(f"  Warning: Failed to process entity: {e}")
                    continue
        
        return records
    
    def get_metadata_batch(self, app_ids: List[str], os_type: str) -> pd.DataFrame:
        """
        Fetch metadata for a batch of apps
        
        Args:
            app_ids: List of app IDs
            os_type: 'ios' or 'android'
            
        Returns:
            DataFrame with metadata
        """
        if not app_ids:
            return pd.DataFrame()
        
        ids_str = ",".join(map(str, app_ids))
        params = {
            'app_ids': ids_str,
            'country': 'US',
            'include_sdk_data': 'false',
            'auth_token': self.token
        }
        
        url = f"{self.base_url}/{os_type}/apps"
        metadata = self._make_request_with_retry(url, params=params)
        
        if not metadata or "apps" not in metadata or not metadata["apps"]:
            return pd.DataFrame()
        
        df = pd.DataFrame(metadata["apps"])
        df["os"] = os_type
        
        # Select required columns
        required_cols = ["app_id", "unified_app_id", "name", "os", "icon_url", "url",
                        "publisher_name", "publisher_country", 
                        "country_release_date", "rating"]
        available_cols = [col for col in required_cols if col in df.columns]
        
        return df[available_cols] if available_cols else pd.DataFrame()
    
    def get_unified_ids(self, app_ids: List[str], batch_size: int = 100, 
                       max_workers: int = 4) -> pd.DataFrame:
        """
        Get unified IDs and metadata for app IDs (auto-detects iOS/Android)
        
        Args:
            app_ids: List of app IDs
            batch_size: Batch size (default: 100)
            max_workers: Number of parallel workers (default: 4)
            
        Returns:
            DataFrame with unified IDs and metadata
        """
        if not app_ids:
            return pd.DataFrame()
        
        # Split into iOS and Android
        ios_apps = [app_id for app_id in app_ids if app_id and str(app_id)[0].isdigit()]
        android_apps = [app_id for app_id in app_ids if app_id and not str(app_id)[0].isdigit()]
        
        print(f"  Processing {len(ios_apps)} iOS apps and {len(android_apps)} Android apps")
        
        all_dataframes = []
        
        # Process in batches with parallel execution
        ios_batches = [ios_apps[i:i + batch_size] for i in range(0, len(ios_apps), batch_size)]
        android_batches = [android_apps[i:i + batch_size] for i in range(0, len(android_apps), batch_size)]
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit iOS tasks
            ios_futures = {
                executor.submit(self.get_metadata_batch, batch, "ios"): f"iOS batch {i+1}"
                for i, batch in enumerate(ios_batches) if batch
            }
            
            # Submit Android tasks
            android_futures = {
                executor.submit(self.get_metadata_batch, batch, "android"): f"Android batch {i+1}"
                for i, batch in enumerate(android_batches) if batch
            }
            
            # Collect results
            all_futures = {**ios_futures, **android_futures}
            
            for future in as_completed(all_futures):
                batch_name = all_futures[future]
                try:
                    result_df = future.result()
                    if not result_df.empty:
                        all_dataframes.append(result_df)
                        print(f"  Completed {batch_name}: {len(result_df)} apps")
                except Exception as e:
                    print(f"  Error processing {batch_name}: {e}")
        
        # Combine all results
        if all_dataframes:
            return pd.concat(all_dataframes, ignore_index=True)
        else:
            return pd.DataFrame(columns=[
                "app_id", "unified_app_id", "name", "os", "icon_url", "url",
                "publisher_name", "publisher_country", 
                "country_release_date", "rating"
            ])
    
    def fetch_all_countries(self, countries: List[str], start_date: str, end_date: str,
                           category: str = '6014') -> Tuple[pd.DataFrame, List[str]]:
        """
        Fetch top apps for multiple countries in parallel
        
        Args:
            countries: List of country codes
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            category: App category (default: '6014' for iOS games)
            
        Returns:
            Tuple of (unified DataFrame, list of unique app IDs)
        """
        all_dataframes = []
        all_app_ids = []
        
        with ThreadPoolExecutor(max_workers=len(countries)) as executor:
            # Submit all country requests
            future_to_country = {
                executor.submit(self.get_top_apps, country, start_date, end_date, category): country 
                for country in countries
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_country):
                country = future_to_country[future]
                try:
                    df = future.result()
                    if not df.empty:
                        all_dataframes.append(df)
                        app_ids = df['app_id'].dropna().astype(str).tolist()
                        all_app_ids.extend(app_ids)
                        print(f"  Completed fetching data for {country}")
                except Exception as e:
                    print(f"  Error fetching data for {country}: {e}")
        
        # Combine all country data
        unified_df = pd.concat(all_dataframes, ignore_index=True) if all_dataframes else pd.DataFrame()
        
        return unified_df, list(set(all_app_ids))  # Remove duplicates


class FacebookAdsClient(APIClient):
    """Facebook Ads API client"""
    
    def __init__(self, token: str):
        config = {
            'api_name': 'facebook',
            'base_url': 'https://graph.facebook.com/v18.0',
            'token': token,
            'headers': {'Content-Type': 'application/json'}
        }
        super().__init__(config)
    
    def get_insights(self, account_id: str, date_preset: str = 'last_30d', 
                    fields: Optional[List[str]] = None) -> List[Dict]:
        """
        Get ad insights for an account
        
        Args:
            account_id: Facebook Ad Account ID
            date_preset: Date preset (e.g., 'last_30d', 'this_month')
            fields: List of fields to retrieve
            
        Returns:
            List of insight data
        """
        if fields is None:
            fields = ['impressions', 'clicks', 'spend', 'actions']
        
        params = {
            'access_token': self.token,
            'date_preset': date_preset,
            'fields': ','.join(fields)
        }
        response = self.get(f'act_{account_id}/insights', params)
        return response.get('data', [])


class TikTokAdsClient(APIClient):
    """TikTok Ads API client"""
    
    def __init__(self, token: str):
        config = {
            'api_name': 'tiktok',
            'base_url': 'https://business-api.tiktok.com/open_api/v1.3',
            'token': {'Access-Token': token},
            'headers': {'Content-Type': 'application/json'}
        }
        super().__init__(config)
    
    def get_reports(self, advertiser_id: str, start_date: str, end_date: str,
                   metrics: Optional[List[str]] = None) -> List[Dict]:
        """
        Get advertising reports
        
        Args:
            advertiser_id: TikTok Advertiser ID
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            metrics: List of metrics to retrieve
            
        Returns:
            List of report data
        """
        if metrics is None:
            metrics = ['spend', 'impressions', 'clicks', 'conversions']
        
        json_data = {
            'advertiser_id': advertiser_id,
            'start_date': start_date,
            'end_date': end_date,
            'metrics': metrics
        }
        response = self.post('reports/integrated/get', json_data=json_data)
        return response.get('data', {}).get('list', [])


def create_client(api_name: str, **kwargs) -> APIClient:
    """
    Factory function to create API clients
    
    Args:
        api_name: Name of API ('sensortower', 'facebook', 'tiktok', or 'custom')
        **kwargs: Additional configuration
        
    Returns:
        APIClient instance
    """
    clients = {
        'sensortower': SensorTowerClient,
        'facebook': FacebookAdsClient,
        'tiktok': TikTokAdsClient,
    }
    
    if api_name in clients:
        return clients[api_name](**kwargs)
    else:
        # Custom API client
        return APIClient(kwargs)
