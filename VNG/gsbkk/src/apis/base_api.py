"""
Base API Client
Provides common HTTP request methods for API clients.
"""

from typing import Any, Dict, List, Optional, Union

import requests


class BaseAPIClient:
    """
    Base API client with common HTTP methods.
    """

    def __init__(self, credentials: Dict[str, Any] = None):
        """
        Initialize base API client.

        Args:
            credentials: Dictionary containing API credentials
        """
        self.credentials = credentials or {}

    def get(
        self,
        url: str,
        params: Dict[str, Any] = None,
        headers: Dict[str, str] = None
    ) -> requests.Response:
        """
        Make a GET request.

        Args:
            url: URL to request
            params: Query parameters
            headers: Request headers

        Returns:
            Response object
        """
        return requests.get(url, params=params, headers=headers)

    def post(
        self,
        url: str,
        data: Dict[str, Any] = None,
        json: Dict[str, Any] = None,
        headers: Dict[str, str] = None
    ) -> requests.Response:
        """
        Make a POST request.

        Args:
            url: URL to request
            data: Form data
            json: JSON data
            headers: Request headers

        Returns:
            Response object
        """
        return requests.post(url, data=data, json=json, headers=headers)

    def _parse_json_response(
        self,
        response: requests.Response
    ) -> list:
        """
        Parse JSON response from API.

        Args:
            response: Response object

        Returns:
            Parsed JSON data

        Raises:
            requests.HTTPError: If response status is not successful
        """
        response.raise_for_status()
        return response.json()

    def authenticate(self) -> bool:
        """
        Authenticate with the API.
        Should be overridden by subclasses.

        Returns:
            True if authentication successful
        """
        raise NotImplementedError("Subclasses must implement authenticate()")
