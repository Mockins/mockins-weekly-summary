from __future__ import annotations

from typing import Any, Dict, List, Optional
import pandas as pd

from weekly_summary.extract.sellercloud.sellercloud_client import SellercloudClient


def _normalize_sku(raw: Any) -> str:
    s = str(raw).strip()

    if s.startswith("ZZZ-"):
        s = s[4:]
    if s.endswith("-LOC"):
        s = s[:-4]

    return s.strip()


def _is_good_sku(s: str) -> bool:
    """
    Real SKUs in your system look like:
      NG-2Z-DRP-AM-48P-42
      MA-RB-25BLK-69
    Bad ones we want to ignore are digits-only ProductIDs like:
      00819867020218
    """
    if not s:
        return False
    if s.isdigit():
        return False
    # require at least one letter (filters out numeric junk)
    return any(ch.isalpha() for ch in s)


def _extract_parent_sku(item: Dict[str, Any]) -> Optional[str]:
    # 1) If it's a child, ShadowOf is the parent SKU we want
    shadow = item.get("ShadowOf")
    if shadow:
        shadow_normalized = _normalize_sku(shadow)
        # Don't use ShadowOf if it's the same as the product's own SKU
        # (that's a self-reference, not a parent-child relationship)
        sku = _normalize_sku(item.get("SKU") or item.get("ManufacturerSKU") or "")
        if shadow_normalized != sku:
            # It's a real parent-child relationship
            return shadow_normalized if _is_good_sku(shadow_normalized) else None

    # 2) Otherwise try a bunch of likely sku fields
    candidates = [
        item.get("SKU"),
        item.get("Sku"),
        item.get("ProductSKU"),
        item.get("ProductSku"),
        item.get("ManufacturerSKU"),
        item.get("MerchantSKU"),
        item.get("SellerSKU"),
        item.get("DefaultSKU"),
        item.get("DefaultSku"),
    ]

    for c in candidates:
        if not c:
            continue
        sku = _normalize_sku(c)
        if _is_good_sku(sku):
            return sku

    # 3) Last resort: skip if nothing matches
    return None


def pull_warehouse_inventory_qty(
    client: SellercloudClient,
    *,
    company_id: int,
    warehouse_id: int,
    page_size: int = 200,
    exclude_zero_inventory: bool = True,
    qty_field: str = "InventoryAvailableQty",
    debug_first_item: bool = False,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    page = 1

    while True:
        data = client.get_inventory_page(
            company_id=company_id,
            warehouse_id=warehouse_id,
            page_number=page,
            page_size=page_size,
            exclude_zero_inventory=exclude_zero_inventory,
        )

        items = data.get("Items") or []

        if debug_first_item and page == 1 and items:
            print("Sellercloud first item keys:", sorted(items[0].keys()))
            print("Sellercloud first item sample:", items[0])

        if not items:
            break

        for it in items:
            # Filter to items in "190 Welles - Bins" or aggregate (WarehouseName=None)
            wh_name = it.get("WarehouseName")
            if wh_name not in (None, "190 Welles - Bins"):
                continue

            sku = _extract_parent_sku(it)
            if not sku:
                continue

            qty_raw = it.get(qty_field)
            try:
                qty = float(qty_raw) if qty_raw is not None else 0.0
            except Exception:
                qty = 0.0

            rows.append({"sku": sku, "sc_warehouse_qty": qty})

        # paging stop: if we got less than a full page, we're done
        if len(items) < page_size:
            break

        page += 1

    if not rows:
        return pd.DataFrame(columns=["sku", "sc_warehouse_qty"])

    df = pd.DataFrame(rows)

    # Group to parent level and take the max (to avoid double-counting parent + bin)
    df = (
        df.groupby("sku", as_index=False)["sc_warehouse_qty"]
        .max()  # Changed from .sum() to .max()
        .sort_values("sc_warehouse_qty", ascending=False)
        .reset_index(drop=True)
    )
    return df


def pull_warehouse_inventory_qty_grid(
    client: SellercloudClient,
    *,
    warehouse_ids: List[int],
    saved_view_id: Optional[int] = None,
    page_size: int = 50,
    exclude_zero_inventory: bool = True,
    qty_field: str = "Available",
    debug_first_item: bool = False,
) -> pd.DataFrame:
    """
    Alternative method using the GetGridData endpoint (dashboard report data).
    This is faster than pull_warehouse_inventory_qty for large datasets.
    
    Args:
        client: SellercloudClient instance
        warehouse_ids: List of warehouse IDs to fetch (e.g., [142])
        saved_view_id: Optional saved view ID for the report (e.g., 187 for Monday Inventory Report)
        page_size: Results per page (default 50)
        exclude_zero_inventory: Whether to exclude zero inventory items
        qty_field: Field name for quantity (default "Available")
        debug_first_item: Print first item keys for debugging
    
    Returns:
        DataFrame with columns ["sku", "sc_warehouse_qty"]
    """
    rows: List[Dict[str, Any]] = []
    page = 1

    while True:
        data = client.get_inventory_grid_data(
            warehouse_ids=warehouse_ids,
            page_number=page,
            results_per_page=page_size,
            saved_view_id=saved_view_id,
            exclude_zero_inventory=exclude_zero_inventory,
            group_by_parent=True,
        )

        # The response structure is different from the regular inventory endpoint
        grid_items = data.get("Data", {}).get("Grid", []) if isinstance(data.get("Data"), dict) else []

        if debug_first_item and page == 1 and grid_items:
            print("GetGridData first item keys:", sorted(grid_items[0].keys()))
            print("GetGridData first item sample:", grid_items[0])

        if not grid_items:
            break

        for it in grid_items:
            sku = _extract_parent_sku(it)
            if not sku:
                continue

            qty_raw = it.get(qty_field)
            try:
                qty = float(qty_raw) if qty_raw is not None else 0.0
            except Exception:
                qty = 0.0

            rows.append({"sku": sku, "sc_warehouse_qty": qty})

        # paging stop: if we got less than a full page, we're done
        if len(grid_items) < page_size:
            break

        page += 1

    if not rows:
        return pd.DataFrame(columns=["sku", "sc_warehouse_qty"])

    df = pd.DataFrame(rows)

    # Group to parent level and take the max (to avoid double-counting parent + bin)
    df = (
        df.groupby("sku", as_index=False)["sc_warehouse_qty"]
        .max()
        .sort_values("sc_warehouse_qty", ascending=False)
        .reset_index(drop=True)
    )
    return df