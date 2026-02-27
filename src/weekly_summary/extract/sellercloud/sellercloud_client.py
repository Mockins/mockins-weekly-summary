"""
SellerCloud API client with token-based authentication and retry logic.
"""
import os
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class SellerCloudClient:
    """
    Handles authentication and API requests to SellerCloud REST API.
    Supports bearer token authentication and automatic retries for rate limits/5xx errors.
    """
    
    def __init__(
        self,
        server_id: str,
        username: str,
        password: str,
        timeout: int = 30,
        max_retries: int = 3,
    ):
        """
        Initialize SellerCloud client.
        
        Args:
            server_id: SellerCloud server ID (from environment)
            username: API username (from environment)
            password: API password (from environment)
            timeout: Request timeout in seconds (default: 30)
            max_retries: Maximum retry attempts for 429/5xx errors (default: 3)
        """
        self.server_id = server_id
        self.username = username
        self.password = password
        self.timeout = timeout
        self.max_retries = max_retries
        self.base_url = f"https://{server_id}.api.sellercloud.com/rest/api"
        self.access_token: Optional[str] = None
        
        # Session with retry strategy for 429 (Too Many Requests) and 5xx errors
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create a requests session with retry strategy."""
        session = requests.Session()
        
        # Retry strategy: retry on 429 and 5xx errors
        retry_strategy = Retry(
            total=self.max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
            backoff_factor=1.0,  # Exponential backoff: 1s, 2s, 4s, etc.
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        return session
    
    def _fetch_token(self) -> str:
        """
        Fetch a new access token from SellerCloud.
        
        Returns:
            Access token string
            
        Raises:
            requests.RequestException: If token request fails
            ValueError: If response doesn't contain access_token
        """
        token_url = f"{self.base_url}/token"
        payload = {
            "Username": self.username,
            "Password": self.password,
        }
        
        logger.info(f"Fetching token from {token_url}")
        
        try:
            response = self.session.post(
                token_url,
                json=payload,
                timeout=self.timeout,
                verify=True,  # Always verify SSL certificates
            )
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Token request failed: {e}")
            raise
        
        try:
            data = response.json()
            token = data.get("access_token")
            if not token:
                raise ValueError(f"No access_token in response: {data}")
            logger.info("Token obtained successfully")
            return token
        except (ValueError, KeyError) as e:
            logger.error(f"Failed to parse token response: {e}")
            raise
    
    def _ensure_token(self) -> str:
        """
        Ensure valid access token. Fetch new one if needed.
        
        Returns:
            Valid access token
        """
        if not self.access_token:
            self.access_token = self._fetch_token()
        return self.access_token
    
    def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> requests.Response:
        """
        Make an authenticated GET request to SellerCloud API.
        
        Args:
            endpoint: API endpoint (e.g., "Inventory/GetAllByView")
            params: Query parameters
            **kwargs: Additional kwargs passed to session.get()
            
        Returns:
            Response object
            
        Raises:
            requests.RequestException: If request fails
        """
        token = self._ensure_token()
        url = f"{self.base_url}/{endpoint}"
        headers = {"Authorization": f"Bearer {token}"}
        
        logger.debug(f"GET {url} with params {params}")
        
        try:
            response = self.session.get(
                url,
                params=params,
                headers=headers,
                timeout=self.timeout,
                verify=True,
                **kwargs
            )
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logger.error(f"GET request failed: {e}")
            raise
    
    def post(
        self,
        endpoint: str,
        json_data: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> requests.Response:
        """
        Make an authenticated POST request to SellerCloud API.
        
        Args:
            endpoint: API endpoint
            json_data: JSON payload
            **kwargs: Additional kwargs passed to session.post()
            
        Returns:
            Response object
            
        Raises:
            requests.RequestException: If request fails
        """
        token = self._ensure_token()
        url = f"{self.base_url}/{endpoint}"
        headers = {"Authorization": f"Bearer {token}"}
        
        logger.debug(f"POST {url} with data {json_data}")
        
        try:
            response = self.session.post(
                url,
                json=json_data,
                headers=headers,
                timeout=self.timeout,
                verify=True,
                **kwargs
            )
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logger.error(f"POST request failed: {e}")
            raise