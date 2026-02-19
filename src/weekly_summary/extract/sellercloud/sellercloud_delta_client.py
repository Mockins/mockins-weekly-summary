"""
Sellercloud Delta API client for real-time inventory data.

This module provides access to Sellercloud's Delta UI API, which serves
real-time inventory data through the GetGridData endpoint.

Authentication: Uses CoreWebAdmin.ASPXAUTH1 session cookie
Base URL: https://sd.delta.sellercloud.com
"""

from __future__ import annotations

import logging
from typing import Any

import requests

logger = logging.getLogger(__name__)


class DeltaAuthenticationError(Exception):
    """Raised when Delta API authentication fails."""
    pass


class DeltaAPIError(Exception):
    """Raised when Delta API returns an error."""
    pass


class SellercloudDeltaClient:
    """
    Client for Sellercloud Delta API using GetGridData endpoint.
    
    This provides real-time inventory data with filtering and pagination.
    Authentication uses CoreWebAdmin.ASPXAUTH1 session cookies.
    """
    
    BASE_URL = "https://sd.delta.sellercloud.com"
    GRID_DATA_ENDPOINT = "/api/Manage/ManageEntity/GetGridData"
    DEFAULT_KIND = 109  # Inventory view kind
    
    def __init__(self, session_cookie: str):
        """
        Initialize with a CoreWebAdmin.ASPXAUTH1 session cookie.
        
        Args:
            session_cookie: The CoreWebAdmin.ASPXAUTH1 cookie value
            
        Raises:
            DeltaAuthenticationError: If cookie is invalid
        """
        if not session_cookie or not isinstance(session_cookie, str):
            raise DeltaAuthenticationError("Invalid session cookie provided")
        
        self.session_cookie = session_cookie
        self.session = requests.Session()
        self.session.cookies.set("CoreWebAdmin.ASPXAUTH1", session_cookie)
        
        logger.info("Initialized SellercloudDeltaClient")
    
    def _validate_cookie(self) -> bool:
        """
        Validate that the session cookie is still valid.
        
        Returns:
            True if cookie is valid, False otherwise
        """
        try:
            # Try a minimal request
            resp = self.session.post(
                f"{self.BASE_URL}{self.GRID_DATA_ENDPOINT}",
                json={
                    "Kind": self.DEFAULT_KIND,
                    "PageNumber": 1,
                    "ResultsPerPage": 1,
                    "SavedViewID": 187,
                    "SelectedFilters": [],
                    "SortColumn": "InventoryDate",
                    "SortDirection": True,
                    "UtcOffset": -300,
                },
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            return resp.status_code == 200
        except Exception as e:
            logger.warning(f"Cookie validation failed: {e}")
            return False
    
    def get_inventory_grid(
        self,
        saved_view_id: int,
        warehouse_id: int = 142,
        page_number: int = 1,
        results_per_page: int = 50,
        exclude_zero_inventory: bool = True,
        date_type: str = "RealTime",
    ) -> dict[str, Any]:
        """
        Get inventory data from a saved view using GetGridData.
        
        Args:
            saved_view_id: The saved view ID to query
            warehouse_id: Warehouse ID to filter by
            page_number: Page number for pagination
            results_per_page: Number of results per page
            exclude_zero_inventory: Whether to exclude zero inventory items
            date_type: Type of date (RealTime, AsOfDate, etc.)
            
        Returns:
            Dict with Data.Grid and Data.Totals
            
        Raises:
            DeltaAPIError: If API call fails
            DeltaAuthenticationError: If session is invalid
        """
        url = f"{self.BASE_URL}{self.GRID_DATA_ENDPOINT}"
        
        payload = {
            "Kind": self.DEFAULT_KIND,
            "IncludeTotals": True,
            "Key": None,
            "PageNumber": page_number,
            "ResultsPerPage": results_per_page,
            "SavedViewID": saved_view_id,
            "SelectedFilters": [
                {
                    "FilterId": "lstCompanies",
                    "FilterPropertyName": "Companies",
                    "FilterSelectedValues": None,
                },
                {
                    "FilterId": "ddlDateType",
                    "FilterPropertyName": "DateType",
                    "FilterSelectedValues": [date_type],
                },
                {
                    "FilterId": "txtAsOfDate",
                    "FilterPropertyName": "AsOfDate",
                    "FilterSelectedValues": [""],
                },
                {
                    "FilterId": "ddlDateRange",
                    "FilterPropertyName": "DateRange",
                    "FilterSelectedValues": [],
                },
                {
                    "FilterId": "dtDate",
                    "FilterPropertyName": "Date",
                    "FilterSelectedValues": [],
                },
                {
                    "FilterId": "lstWarehouses",
                    "FilterPropertyName": "Warehouses",
                    "FilterSelectedValues": [str(warehouse_id)],
                },
                {
                    "FilterId": "ddlExcludeZeroInventory",
                    "FilterPropertyName": "ExcludeZeroInventory",
                    "FilterSelectedValues": ["true" if exclude_zero_inventory else "false"],
                },
                {
                    "FilterId": "ddlGroupByParent",
                    "FilterPropertyName": "GroupByParent",
                    "FilterSelectedValues": ["true"],
                },
            ],
            "SortColumn": "InventoryDate",
            "SortDirection": True,
            "UtcOffset": -300,
        }
        
        headers = {
            "Content-Type": "application/json",
            "X-Requested-With": "XMLHttpRequest",
        }
        
        try:
            resp = self.session.post(url, json=payload, headers=headers, timeout=60)
            resp.raise_for_status()
            
            data = resp.json()
            
            if not data or "Data" not in data:
                raise DeltaAPIError("Invalid response format from Delta API")
            
            return data
        
        except requests.exceptions.HTTPError as e:
            if resp.status_code == 401:
                raise DeltaAuthenticationError(
                    "Session cookie is invalid or expired. Please refresh the cookie."
                ) from e
            raise DeltaAPIError(f"Delta API HTTP error: {e}") from e
        except requests.exceptions.RequestException as e:
            raise DeltaAPIError(f"Failed to connect to Delta API: {e}") from e
    
    def get_all_inventory(
        self,
        saved_view_id: int,
        warehouse_id: int = 142,
        results_per_page: int = 50,
    ) -> list[dict[str, Any]]:
        """
        Get all inventory items by paginating through all results.
        
        Args:
            saved_view_id: The saved view ID to query
            warehouse_id: Warehouse ID to filter by
            results_per_page: Number of results per page
            
        Returns:
            List of all inventory items
            
        Raises:
            DeltaAPIError: If any API call fails
        """
        all_items: list[dict[str, Any]] = []
        page = 1
        
        logger.info(f"Fetching all inventory from saved view {saved_view_id}")
        
        while True:
            logger.debug(f"Fetching page {page}...")
            
            data = self.get_inventory_grid(
                saved_view_id=saved_view_id,
                warehouse_id=warehouse_id,
                page_number=page,
                results_per_page=results_per_page,
            )
            
            grid = data.get("Data", {}).get("Grid") or []
            
            if not grid:
                logger.debug(f"No more items at page {page}")
                break
            
            all_items.extend(grid)
            logger.debug(f"Page {page}: {len(grid)} items")
            page += 1
        
        logger.info(f"Total inventory items fetched: {len(all_items)}")
        return all_items
    
    def search_inventory(
        self,
        saved_view_id: int,
        search_term: str,
        search_field: str = "Sku",
        warehouse_id: int = 142,
    ) -> list[dict[str, Any]]:
        """
        Search for inventory items by a specific field.
        
        Args:
            saved_view_id: The saved view ID to query
            search_term: The term to search for
            search_field: The field to search in (default: Sku)
            warehouse_id: Warehouse ID to filter by
            
        Returns:
            List of matching items
        """
        all_items = self.get_all_inventory(saved_view_id, warehouse_id)
        
        return [
            item
            for item in all_items
            if search_term.lower() in str(item.get(search_field, "")).lower()
        ]