"""
Sensor Tower API Client
"""

import json
from typing import Any, Dict, List, Optional, Union

from airflow.models import Variable

from apis.base_api import BaseAPIClient


class SensorTowerAPI(BaseAPIClient):
    """
    Sensor Tower API client.

    Handles authentication and data retrieval from Sensor Tower API.
    """

    def __init__(self, credentials: Dict[str, Any] = None):
        """
        Initialize Sensor Tower API client.

        Args:
            credentials: Dictionary containing 'api_key'
        """
        super().__init__(credentials)
        self.base_url = "https://api.sensortower.com/v1"
        # self.api_key = self.credentials.get('api_key', '')
        self.api_key = Variable.get("SENSOR_TOWER_API_TOKEN")
        self.rate_limit_delay = 0.5  # Sensor Tower rate limits

    def authenticate(self) -> bool:
        """
        Authenticate with Sensor Tower.

        Sensor Tower uses API key in headers, so just verify we have it.
        """
        if not self.api_key:
            raise ValueError("Sensor Tower API key is required in credentials")
        return True

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with API key."""
        return {
            'Content-Type': 'application/json'
        }

    def get_data(
        self,
        endpoint: str,
        params: Dict[str, Any] = None
    ) -> Any:
        """
        Get data from Sensor Tower endpoint.

        Args:
            endpoint: API endpoint (e.g., '/ios/rankings/get_category_rankings')
            params: Query parameters

        Returns:
            API response data (can be dict or list depending on endpoint)
        """
        url = f"{self.base_url}{endpoint}"

        response = self.get(
            url,
            params=params,
            headers=self._get_headers()
        )

        return self._parse_json_response(response)

    def get_top_apps(
        self,
        start_date: str,
        end_date: str,
        region: str = 'US',
        os: str = 'ios'
    ) -> Any:
        """
        Get top apps rankings using OS-specific sales report estimates comparison.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            region: Country code (e.g., 'US', 'VN', 'TH')
            os: Operating system ('ios' or 'android')
            category: App category code (default '6014' for games)
            limit: Number of results (default 1000 per country)
            measure: Measure type ('revenue', 'downloads', etc.)

        Returns:
            Any: List of top apps
        """
        endpoint = f"/{os}/sales_report_estimates_comparison_attributes"
        if os == 'ios':
            category = '6014'
            device_type = 'total'
        else:
            category = 'game'
            device_type = None

        params = {
            'date': start_date,
            'end_date': end_date,
            'regions': region,
            'category': category,
            'limit': 5,
            'measure': 'revenue',
            'comparison_attribute': 'absolute',
            'device_type': device_type,
            'time_range': 'month',
            'custom_tags_mode': 'include_unified_apps',
            'data_model': 'DM_2025_Q2',
            'auth_token': self.api_key
        }

        response = self.get_data(endpoint, params)
        return response

    def get_new_apps(
        self,
        start_date: str,
        os: str = 'ios',
        limit: int = 10000
    ) -> Dict[str, Any]:
        """
        Get newly released apps with pagination.

        Args:
            start_date: Start date (YYYY-MM-DD)
            os: Operating system ('ios' or 'android')
            limit: Number of results per page (default 10000)

        Returns:
            Dict with 'ids' (list of app IDs) and 'date_range' (date string)
        """
        endpoint = f"/{os}/apps/app_ids"

        # Set category based on OS
        if os == 'ios':
            category = '6014'
        else:
            category = 'game'

        all_ids = []
        date_range_value = None
        offset = 0

        while True:
            params = {
                'category': category,
                'start_date': start_date,
                'limit': limit,
                'offset': offset,
                'auth_token': self.api_key
            }

            response = self.get_data(endpoint, params)

            # Check if response has data
            if not response:
                break

            # Extract ids and date_range from response
            if isinstance(response, dict):
                ids = response.get('ids', [])
                if not ids:
                    break

                all_ids.extend(ids)

                # Get date_range (same for all pages, just extract once)
                if date_range_value is None and 'date_range' in response:
                    # Extract just the date part (YYYY-MM-DD) from first element
                    date_range_raw = response['date_range'][0]
                    date_range_value = date_range_raw.split('T')[0]

                # If we got less than limit, we've reached the end
                if len(ids) < limit:
                    break
            else:
                break

            # Move to next page
            offset += limit

        return {
            'date_range': date_range_value if date_range_value else start_date,
            'ids': all_ids
        }

    def get_app_details(
        self,
        app_id: str,
        device: str = 'iphone'
    ) -> Dict[str, Any]:
        """
        Get detailed information about an app.

        Args:
            app_id: App identifier
            device: Device type ('iphone' or 'android')

        Returns:
            App details
        """
        endpoint = f"/{device}/apps/get_app"

        params = {
            'app_id': app_id
        }

        return self.get_data(endpoint, params)

    def get_performance(
        self,
        app_ids: List[str],
        os: str,
        start_date: str,
        end_date: str,
        date_granularity: str = 'monthly',
        country: Union[str, List[str], None] = None
    ) -> List[Dict[str, Any]]:
        """
        Get performance data (revenue and downloads) for multiple apps.
        Batches app_ids into groups of 100 and uses multi-threading for parallel requests.

        Args:
            app_ids: List of app IDs
            os: Operating system ('ios' or 'android')
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            country: Single country string ('VN'), list of countries (['VN', 'TH']), or None (defaults to ['VN'])

        Returns:
            Flat list of performance records: [{app 1}, {app 2}, ...]
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        endpoint = f"/{os}/sales_report_estimates"

        # Handle country parameter flexibility
        if country is None:
            countries = ['VN']  # Default for testing
        elif isinstance(country, str):
            countries = [country]  # Single country -> list with one element
        elif isinstance(country, list):
            countries = country  # Multiple countries -> use as-is
        else:
            raise ValueError(
                f"Invalid country parameter type: {type(country)}. Expected str, list, or None.")

        # Split app_ids into batches of 100
        batches = [app_ids[i:i+100] for i in range(0, len(app_ids), 100)]

        all_results = []

        def fetch_batch(batch):
            """Fetch performance data for a batch of app IDs"""
            params = {
                'app_ids': ','.join(str(app_id) for app_id in batch),
                'countries': countries,
                'date_granularity': date_granularity,
                'start_date': start_date,
                'end_date': end_date,
                'data_model': 'DM_2025_Q2',
                'auth_token': self.api_key
            }

            try:
                response = self.get_data(endpoint, params)
                # Response is already a list: [{app 1}, {app 2}, ...]
                if isinstance(response, list):
                    return response
                return []
            except Exception as e:
                print(f"Error fetching batch: {e}")
                return []

        # Use ThreadPoolExecutor to fetch batches in parallel
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(
                fetch_batch, batch): batch for batch in batches}

            for future in as_completed(futures):
                batch_results = future.result()
                all_results.extend(batch_results)

        return all_results

    def get_apps_metadata(
        self,
        app_ids: List[str],
        country: str,
        os: str = 'ios'
    ) -> list:
        """
        Get app metadata for multiple apps (max 100 per call).

        Args:
            app_ids: List of app IDs (max 100)
            country: Country code (e.g., 'US', 'VN', 'TH')
            os: Operating system ('ios' or 'android')

        Returns:
            Dict with 'apps' key containing list of app metadata
        """
        endpoint = f"/{os}/apps"
        app_ids_str = ','.join(str(app_id) for app_id in app_ids)

        params = {
            'app_ids': app_ids_str,
            'country': country,
            'auth_token': self.api_key
        }

        response = self.get_data(endpoint, params)
        return response['apps']

    def get_game_tags(
        self,
        app_ids: List[str]
    ) -> list:
        """
        Get game tags for multiple apps (max 100 per call).

        Args:
            app_ids: List of app IDs (max 100, must be same OS)

        Returns:
            Dict with 'data' key containing list of apps with their tags
        """
        endpoint = "/app_tag/tags_for_apps"

        # Join app_ids with commas
        app_ids_str = ','.join(str(app_id) for app_id in app_ids)

        params = {
            'app_ids': app_ids_str,
            'field_categories[]': 'gaming',
            'auth_token': self.api_key
        }

        response = self.get_data(endpoint, params)
        return response['data']
