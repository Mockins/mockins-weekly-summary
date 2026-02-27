"""
Pull inventory from SellerCloud using saved views and normalize to DataFrame.
"""
import logging
from typing import List, Dict, Any
import pandas as pd

try:
    from .sellercloud_client import SellerCloudClient
except ImportError:
    from weekly_summary.extract.sellercloud.sellercloud_client import SellerCloudClient

logger = logging.getLogger(__name__)


def pull_190_welles_inventory(
    server_id: str,
    username: str,
    password: str,
    view_id: int = 187,
    page_size: int = 50,
) -> pd.DataFrame:
    """
    Pull "190 Welles Inventory" from SellerCloud saved view.
    
    Uses the "Monday Inventory Report" saved view (ID 187) which is pre-configured with:
    - Warehouse: 190 Welles - bins (142)
    - Exclude zero inventory: Yes
    - Group by parent SKU: Yes
    
    API Parameters (per SellerCloud documentation):
    - viewID (required): The ID of the existing Inventory saved view
    - pageNumber (required): Page number
    - pageSize (required): Page size (max 50)
    
    Note: API returns duplicate entries for same SKU (different channels/variants).
    We keep only the first occurrence for each unique SKU.
    
    Args:
        server_id: SellerCloud server ID
        username: API username
        password: API password
        view_id: Saved view ID (default: 187 for "Monday Inventory Report")
        page_size: Items per page (default: 50, max is 50)
        
    Returns:
        DataFrame with columns:
            - SKU: Parent SKU (ShadowOf if exists, else ManufacturerSKU)
            - Welles190Qty: Available inventory quantity (InventoryAvailableQty)
    """
    # Enforce API limit of 50 per page
    if page_size > 50:
        logger.warning(f"page_size {page_size} exceeds API max of 50, setting to 50")
        page_size = 50
    
    logger.info(f"Pulling from saved view {view_id} (Monday Inventory Report)")
    logger.info(f"Page size: {page_size} (API max: 50)")
    
    client = SellerCloudClient(server_id, username, password)
    all_items: List[Dict[str, Any]] = []
    
    page_number = 1
    while True:
        logger.debug(f"Fetching page {page_number}")
        
        try:
            # Only pass the 3 required parameters per API documentation
            response = client.get(
                "Inventory/GetAllByView",
                params={
                    "viewID": view_id,
                    "pageNumber": page_number,
                    "pageSize": page_size,
                }
            )
        except Exception as e:
            logger.error(f"Failed to fetch page {page_number}: {e}")
            raise
        
        try:
            data = response.json()
        except ValueError as e:
            logger.error(f"Invalid JSON response on page {page_number}: {e}")
            raise
        
        items = data.get("Items", [])
        
        if not items:
            logger.info(f"Pagination complete at page {page_number}")
            break
        
        logger.debug(f"Page {page_number}: {len(items)} items")
        all_items.extend(items)
        page_number += 1
    
    logger.info(f"Total items fetched from API: {len(all_items)}")
    
    # Extract parent SKU and inventory quantity
    rows = []
    stats = {
        "empty_sku": 0,
        "zero_qty": 0,
        "processed": 0,
    }
    
    for item in all_items:
        try:
            qty = int(item.get("InventoryAvailableQty") or 0)
            
            # Only include items with positive inventory
            if qty <= 0:
                stats["zero_qty"] += 1
                continue
            
            # Get parent SKU (prefer ShadowOf over ManufacturerSKU)
            sku = item.get("ShadowOf") or item.get("ManufacturerSKU")
            sku = str(sku or "").strip()
            
            if not sku:
                stats["empty_sku"] += 1
                continue
            
            # Add row
            rows.append({
                "SKU": sku,
                "Welles190Qty": qty,
            })
            stats["processed"] += 1
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Error parsing item: {e}")
            continue
    
    logger.info(f"Processing stats:")
    logger.info(f"  - Processed: {stats['processed']} items with positive inventory")
    logger.info(f"  - Skipped (empty SKU): {stats['empty_sku']}")
    logger.info(f"  - Skipped (zero qty): {stats['zero_qty']}")
    
    # Convert to DataFrame
    df = pd.DataFrame(rows)
    
    logger.info(f"Total rows (before dedup): {len(df)}")
    
    # Remove duplicates - keep only FIRST occurrence of each SKU
    # (API returns same SKU multiple times for different channels/variants)
    df_dedup = df.drop_duplicates(subset=['SKU'], keep='first')
    
    logger.info(f"After removing duplicates: {len(df_dedup)} unique SKUs")
    logger.info(f"Removed {len(df) - len(df_dedup)} duplicate SKU entries")
    
    # Sort by SKU for consistency
    df_dedup = df_dedup.sort_values("SKU").reset_index(drop=True)
    
    logger.info(f"Final DataFrame: {len(df_dedup)} rows x {len(df_dedup.columns)} columns")
    return df_dedup