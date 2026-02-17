from __future__ import annotations

import os
from dotenv import load_dotenv

from weekly_summary.extract.sellercloud.sellercloud_client import SellercloudClient, SellercloudConfig
from weekly_summary.extract.sellercloud.sc_run import pull_warehouse_inventory_qty

from weekly_summary.extract.amazon.pull_restock_inventory import pull_restock_inventory_raw
from weekly_summary.transform.restock_inventory import load_and_normalize_restock
from weekly_summary.transform.current_stock import compute_current_stock
from weekly_summary.helpers.asin_sku_mapping import load_asin_sku_mapping


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
    sc_cfg = SellercloudConfig(
        rest_api_base_url=os.environ["SELLERCLOUD_REST_API_BASE_URL"],
        username=os.environ["SELLERCLOUD_USERNAME"],
        password=os.environ["SELLERCLOUD_PASSWORD"],
    )
    sc_client = SellercloudClient(sc_cfg)

    sc_company_id = int(os.environ.get("SELLERCLOUD_COMPANY_ID", "177"))
    sc_warehouse_id = int(os.environ.get("SELLERCLOUD_WAREHOUSE_ID", "142"))

    sc_df = pull_warehouse_inventory_qty(
        sc_client,
        company_id=sc_company_id,
        warehouse_id=sc_warehouse_id,
        page_size=200,
        exclude_zero_inventory=True,
        qty_field="InventoryAvailableQty",
        debug_first_item=False,
    )
    print("Sellercloud rows:", len(sc_df))

    # 6) Merge Sellercloud qty into the restock-derived data on SKU
    df_final = df3.merge(sc_df, on="sku", how="left")
    df_final["sc_warehouse_qty"] = df_final["sc_warehouse_qty"].fillna(0.0)

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