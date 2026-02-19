from __future__ import annotations

import os
from dotenv import load_dotenv

from weekly_summary.extract.sellercloud.sellercloud_client import SellercloudClient, SellercloudConfig
from weekly_summary.extract.amazon.pull_restock_inventory import pull_restock_inventory_raw
from weekly_summary.transform.restock_inventory import load_and_normalize_restock
from weekly_summary.transform.current_stock import compute_current_stock
from weekly_summary.helpers.asin_sku_mapping import load_asin_sku_mapping
from typing import Any, Dict, List, Optional
import pandas as pd


def _normalize_sku(raw: Any) -> str:
    s = str(raw).strip()

    # KEEP ZZZ prefix - don't remove it
    # Just remove -LOC suffix
    if s.endswith("-LOC"):
        s = s[:-4]

    return s.strip()


def _is_good_sku(s: str) -> bool:
    """
    Real SKUs in your system look like:
      NG-2Z-DRP-AM-48P-42
      MA-RB-25BLK-69
      ZZZ-MA-RB-25BLK-69
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

    # Group by exact SKU (including ZZZ prefix) and take max
    # This keeps ZZZ-MA-RB-25BLK-69 and MA-RB-25BLK-69 as separate rows
    df = (
        df.groupby("sku", as_index=False)["sc_warehouse_qty"]
        .max()
        .sort_values("sc_warehouse_qty", ascending=False)
        .reset_index(drop=True)
    )
    return df


def main() -> None:
    load_dotenv(override=True)
    print("weekly_summary.run: starting")

    # 1) Pull restock report (reuse cached so we avoid quota)
    pulled = pull_restock_inventory_raw(reuse_if_exists=True)
    print(f"Using restock raw: {pulled.raw_path}")

    # 2) Normalize report into dataframe
    df = load_and_normalize_restock(pulled.raw_path)
    print("Restock normalized rows:", len(df))

    # 3) Compute inbound/current_stock/current_stock_per_6
    df2 = compute_current_stock(df)

    # 4) Join in SKU using ASIN -> SKU mapping (keep BOTH columns)
    mapping = load_asin_sku_mapping()
    print("ASIN->SKU mapping rows:", len(mapping))

    df3 = (
        df2.merge(
            mapping[["ASIN", "SKU"]],
            left_on="asin",
            right_on="ASIN",
            how="left",
        )
        .rename(columns={"SKU": "sku", "ASIN": "asin_from_sheet"})
    )
    print("Amazon + SKU mapping rows:", len(df3))

    # 5) Sellercloud: pull warehouse inventory with "Available" qty
    print("\nLoading Sellercloud config...")
    sc_cfg = SellercloudConfig(
        rest_api_base_url=os.environ["SELLERCLOUD_REST_API_BASE_URL"],
        username=os.environ["SELLERCLOUD_USERNAME"],
        password=os.environ["SELLERCLOUD_PASSWORD"],
    )
    sc_client = SellercloudClient(sc_cfg)

    sc_company_id = int(os.environ.get("SELLERCLOUD_COMPANY_ID", "177"))
    sc_warehouse_id = int(os.environ.get("SELLERCLOUD_WAREHOUSE_ID", "142"))

    print(f"Pulling inventory for warehouse {sc_warehouse_id}...")
    sc_df = pull_warehouse_inventory_qty(
        sc_client,
        company_id=sc_company_id,
        warehouse_id=sc_warehouse_id,
        page_size=200,
        exclude_zero_inventory=True,
        qty_field="InventoryAvailableQty",
        debug_first_item=False,
    )
    print(f"Sellercloud rows: {len(sc_df)}")
    print("\nSellercloud SKUs returned:")
    print(sc_df[["sku", "sc_warehouse_qty"]].to_string(index=False))

    # 6) Merge Sellercloud qty into the restock-derived data on SKU
    print(f"\n\nAmazon SKUs (unique):")
    print(df3[["sku"]].drop_duplicates().head(20).to_string(index=False))
    
    df_final = df3.merge(sc_df, on="sku", how="left")
    df_final["sc_warehouse_qty"] = df_final["sc_warehouse_qty"].fillna(0.0)

    print(f"\nMatched rows: {(df_final['sc_warehouse_qty'] > 0).sum()}")

    # 7) Display results: Amazon columns + Sellercloud available qty
    output_cols = [
        "sku",
        "asin",
        "inventory_available",
        "fc_transfer",
        "fc_processing",
        "inbound",
        "current_stock_per_6",
        "sc_warehouse_qty",
    ]
    output_cols = [c for c in output_cols if c in df_final.columns]
    
    print("\n=== FINAL REPORT: Amazon + Sellercloud Inventory ===")
    print(df_final[output_cols].sort_values("sc_warehouse_qty", ascending=False).head(50).to_string(index=False))

    print("\nweekly_summary.run: done")


if __name__ == "__main__":
    main()