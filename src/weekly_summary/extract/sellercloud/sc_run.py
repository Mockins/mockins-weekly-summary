from __future__ import annotations

from typing import Any, Dict, List, Optional
import pandas as pd

from weekly_summary.extract.sellercloud.sellercloud_client import SellercloudClient


def _normalize_sku(raw: Any) -> str:
    s = str(raw).strip()
    
    # Remove -LOC suffix but KEEP ZZZ prefix
    if s.endswith("-LOC"):
        s = s[:-4]

    return s.strip()


def _is_good_sku(s: str) -> bool:
    """Filter out numeric-only SKUs"""
    if not s:
        return False
    if s.isdigit():
        return False
    return any(ch.isalpha() for ch in s)


def pull_warehouse_inventory_qty(
    client: SellercloudClient,
    *,
    company_id: int,
    warehouse_id: int,
    qty_field: str = "InventoryAvailableQty",
    exclude_zero_inventory: bool = True,
    debug_pagination: bool = False,
) -> pd.DataFrame:
    """
    Pull warehouse inventory quantity from Sellercloud.
    
    Gets all items from inventory page, filtering for items with ANY non-zero quantity
    (Physical, Reserved, or Available), then extracts SKU and available quantity.
    This matches the "Inventory by Product Detail" report.
    
    Args:
        client: SellercloudClient instance
        company_id: Company ID (e.g., 177)
        warehouse_id: Warehouse ID (e.g., 142)
        qty_field: Field name for quantity (default "InventoryAvailableQty")
        exclude_zero_inventory: Filter out items with all zero quantities (default True)
        debug_pagination: Print pagination info
    
    Returns:
        DataFrame with columns ["sku", "sc_warehouse_qty"]
    """
    rows: List[Dict[str, Any]] = []
    page = 1
    
    if debug_pagination:
        print("Fetching all inventory pages...\n")
    
    while True:
        data = client.get_inventory_page(
            company_id=company_id,
            warehouse_id=warehouse_id,
            page_number=page,
            page_size=50,
            exclude_zero_inventory=False,  # Get all, we'll filter client-side
        )
        
        items = data.get("Items") or []
        
        if debug_pagination:
            print(f"  Page {page}: Got {len(items)} items")
        
        if not items:
            break
        
        for item in items:
            # Check if ANY quantity field is non-zero (matches report behavior)
            physical_qty = float(item.get("PhysicalQty", 0) or 0)
            reserved_qty = float(item.get("ReservedQty", 0) or 0)
            available_qty = float(item.get(qty_field, 0) or 0)
            
            # Filter: only include if ANY quantity is non-zero
            if exclude_zero_inventory:
                if physical_qty == 0 and reserved_qty == 0 and available_qty == 0:
                    continue
            
            # Extract SKU - use ManufacturerSKU (has -LOC duplicates that we'll normalize)
            sku = item.get("ManufacturerSKU") or item.get("SKU")
            
            if not sku:
                continue
            
            sku = _normalize_sku(sku)
            
            # Filter out numeric-only SKUs
            if not _is_good_sku(sku):
                continue
            
            rows.append({"sku": sku, "sc_warehouse_qty": available_qty})
        
        if len(items) < 50:
            break
        
        page += 1
    
    if debug_pagination:
        print(f"  Total pages processed: {page-1}\n")
    
    if not rows:
        return pd.DataFrame(columns=["sku", "sc_warehouse_qty"])
    
    df = pd.DataFrame(rows)
    
    # Dedup (group by sku, take max qty)
    df = (
        df.groupby("sku", as_index=False)["sc_warehouse_qty"]
        .max()
        .sort_values("sc_warehouse_qty", ascending=False)
        .reset_index(drop=True)
    )
    
    return df