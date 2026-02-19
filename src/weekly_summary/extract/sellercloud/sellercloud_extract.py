"""
Extract Sellercloud data for weekly reports using Delta API.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

from dotenv import load_dotenv

from .sellercloud_delta_client import SellercloudDeltaClient, DeltaAuthenticationError, DeltaAPIError

logger = logging.getLogger(__name__)


@dataclass
class InventoryItem:
    """Represents a single inventory item."""
    sku: str
    product_name: str
    warehouse: str
    physical: int
    reserved: int
    available: int
    cost: float
    value: float
    
    @classmethod
    def from_delta_item(cls, item: dict[str, Any]) -> "InventoryItem":
        """Convert Delta API response to InventoryItem."""
        return cls(
            sku=item.get("Sku", ""),
            product_name=item.get("Product_Name", ""),
            warehouse=item.get("Warehouse", ""),
            physical=item.get("Physical", 0),
            reserved=item.get("Reserved", 0),
            available=item.get("Available", 0),
            cost=float(item.get("Cost", 0)),
            value=float(item.get("Value", 0)),
        )


def get_sellercloud_delta_client() -> SellercloudDeltaClient:
    """
    Initialize Sellercloud Delta client from environment.
    
    Requires SELLERCLOUD_DELTA_SESSION_COOKIE in .env
    
    Returns:
        Initialized SellercloudDeltaClient
        
    Raises:
        ValueError: If session cookie not found in environment
    """
    load_dotenv(override=True)
    
    cookie = os.environ.get("SELLERCLOUD_DELTA_SESSION_COOKIE")
    
    if not cookie:
        raise ValueError(
            "SELLERCLOUD_DELTA_SESSION_COOKIE not found in environment. "
            "Set it in .env file or as environment variable."
        )
    
    return SellercloudDeltaClient(cookie)


def get_inventory_data(
    saved_view_id: int = 187,
    warehouse_id: int = 142,
) -> list[InventoryItem]:
    """
    Get all current inventory from Sellercloud Delta.
    
    Args:
        saved_view_id: The saved view ID (default: 187 for inventory report)
        warehouse_id: The warehouse ID (default: 142)
        
    Returns:
        List of InventoryItem objects
        
    Raises:
        DeltaAuthenticationError: If session cookie is invalid
        DeltaAPIError: If API call fails
    """
    try:
        client = get_sellercloud_delta_client()
        items = client.get_all_inventory(
            saved_view_id=saved_view_id,
            warehouse_id=warehouse_id,
        )
        return [InventoryItem.from_delta_item(item) for item in items]
    except DeltaAuthenticationError as e:
        logger.error(f"Authentication failed: {e}")
        raise
    except DeltaAPIError as e:
        logger.error(f"API error: {e}")
        raise


def get_inventory_by_sku(
    sku: str,
    saved_view_id: int = 187,
    warehouse_id: int = 142,
) -> InventoryItem | None:
    """
    Get inventory for a specific SKU.
    
    Args:
        sku: The SKU to search for
        saved_view_id: The saved view ID
        warehouse_id: The warehouse ID
        
    Returns:
        InventoryItem if found, None otherwise
    """
    client = get_sellercloud_delta_client()
    results = client.search_inventory(
        saved_view_id=saved_view_id,
        search_term=sku,
        search_field="Sku",
        warehouse_id=warehouse_id,
    )
    
    if results:
        return InventoryItem.from_delta_item(results[0])
    return None


def get_inventory_totals(
    saved_view_id: int = 187,
    warehouse_id: int = 142,
) -> dict[str, float]:
    """
    Get inventory totals (total value, cost, etc).
    
    Args:
        saved_view_id: The saved view ID
        warehouse_id: The warehouse ID
        
    Returns:
        Dict with totals
    """
    client = get_sellercloud_delta_client()
    data = client.get_inventory_grid(
        saved_view_id=saved_view_id,
        warehouse_id=warehouse_id,
        results_per_page=1,
    )
    
    totals = data.get("Data", {}).get("Totals", {})
    
    return {
        "total_physical": totals.get("Physical", 0),
        "total_reserved": totals.get("Reserved", 0),
        "total_available": totals.get("Available", 0),
        "total_cost": float(totals.get("Cost", 0)),
        "total_value": float(totals.get("Value", 0)),
    }