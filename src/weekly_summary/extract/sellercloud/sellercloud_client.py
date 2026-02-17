from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, List

import requests


@dataclass(frozen=True)
class SellercloudConfig:
    rest_api_base_url: str  # e.g. https://sd.api.sellercloud.com/rest
    username: str
    password: str
    timeout_s: int = 60


class SellercloudClient:
    def __init__(self, config: SellercloudConfig) -> None:
        self.config = config
        self.base_url = config.rest_api_base_url.rstrip("/")
        self._token: Optional[str] = None

        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})

    def get_token(self) -> str:
        if self._token:
            return self._token

        url = f"{self.base_url}/api/token"
        resp = self._session.post(
            url,
            data={"username": self.config.username, "password": self.config.password},
            timeout=self.config.timeout_s,
        )
        resp.raise_for_status()
        data = resp.json()

        token = data.get("access_token") or data.get("AccessToken") or data.get("token")
        if not token:
            raise RuntimeError(f"Sellercloud token missing in response: {data}")

        self._token = token
        return token

    def _auth_headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.get_token()}"}

    def get_inventory_page(
        self,
        *,
        company_id: int,
        warehouse_id: int,
        page_number: int,
        page_size: int,
        exclude_zero_inventory: bool = True,
    ) -> Dict[str, Any]:
        """
        Matches the Postman call you used:
        GET /api/inventory?companyID=177&warehouses=142&pageNumber=1&pageSize=200&excludeZeroInventory=true
        """
        url = f"{self.base_url}/api/inventory"
        params = {
            "companyID": company_id,
            "warehouses": warehouse_id,
            "pageNumber": page_number,
            "pageSize": page_size,
        }
        if exclude_zero_inventory:
            params["excludeZeroInventory"] = "true"

        resp = self._session.get(
            url,
            params=params,
            headers=self._auth_headers(),
            timeout=self.config.timeout_s,
        )
        resp.raise_for_status()
        return resp.json()

    def get_inventory_for_product_warehouse(
        self,
        *,
        product_id: str,
        warehouse_id: int,
    ) -> Dict[str, Any]:
        """
        Matches your working Postman call:
        GET /api/Inventory/{productId}/Warehouses/{warehouseId}

        Example:
        /api/Inventory/WM-K42/Warehouses/142  -> { AvailableQty: 1189, ... }
        """
        product_id = str(product_id).strip()
        url = f"{self.base_url}/api/Inventory/{product_id}/Warehouses/{warehouse_id}"

        resp = self._session.get(
            url,
            headers=self._auth_headers(),
            timeout=self.config.timeout_s,
        )
        resp.raise_for_status()
        return resp.json()

    def get_inventory_grid_data(
        self,
        *,
        warehouse_ids: List[int],
        page_number: int = 1,
        results_per_page: int = 50,
        saved_view_id: Optional[int] = None,
        exclude_zero_inventory: bool = True,
        group_by_parent: bool = True,
        date_type: str = "RealTime",
    ) -> Dict[str, Any]:
        """
        Fetch inventory data using the GetGridData endpoint.
        This matches the dashboard report you were viewing.
        
        POST /api/Manage/ManageEntity/GetGridData
        
        Args:
            warehouse_ids: List of warehouse IDs to filter by (e.g., [142])
            page_number: Page number for pagination (default 1)
            results_per_page: Results per page (default 50)
            saved_view_id: Optional saved view ID (e.g., 187 for your inventory report)
            exclude_zero_inventory: Whether to exclude zero inventory items
            group_by_parent: Whether to group by parent SKU
            date_type: Date type filter (e.g., "RealTime", "AsOfDate")
        
        Returns:
            Dictionary with "Grid" (list of inventory items) and "Totals"
        """
        # Build the selected filters
        selected_filters = [
            {
                "FilterId": "lstCompanies",
                "FilterPropertyName": "Companies",
                "FilterSelectedValues": None
            },
            {
                "FilterId": "ddlDateType",
                "FilterPropertyName": "DateType",
                "FilterSelectedValues": [date_type]
            },
            {
                "FilterId": "txtAsOfDate",
                "FilterPropertyName": "AsOfDate",
                "FilterSelectedValues": [""]
            },
            {
                "FilterId": "ddlDateRange",
                "FilterPropertyName": "DateRange",
                "FilterSelectedValues": []
            },
            {
                "FilterId": "dtDate",
                "FilterPropertyName": "Date",
                "FilterSelectedValues": []
            },
            {
                "FilterId": "lstWarehouses",
                "FilterPropertyName": "Warehouses",
                "FilterSelectedValues": [str(wid) for wid in warehouse_ids]
            },
            {
                "FilterId": "ddlExcludeZeroInventory",
                "FilterPropertyName": "ExcludeZeroInventory",
                "FilterSelectedValues": ["true" if exclude_zero_inventory else "false"]
            },
            {
                "FilterId": "ddlGroupByParent",
                "FilterPropertyName": "GroupByParent",
                "FilterSelectedValues": ["true" if group_by_parent else "false"]
            },
        ]
        
        payload = {
            "Kind": 109,  # Inventory report kind
            "Key": None,
            "PageNumber": page_number,
            "ResultsPerPage": results_per_page,
            "SavedViewID": saved_view_id,
            "IncludeTotals": True,
            "SelectedFilters": selected_filters,
            "SortColumn": "InventoryDate",
            "SortDirection": True,
            "UtcOffset": -300,  # EST offset in minutes
        }
        
        # Correct URL - keep /rest in the path
        url = f"{self.base_url}/api/Manage/ManageEntity/GetGridData"
        
        resp = self._session.post(
            url,
            json=payload,
            headers=self._auth_headers(),
            timeout=self.config.timeout_s,
        )
        resp.raise_for_status()
        return resp.json()