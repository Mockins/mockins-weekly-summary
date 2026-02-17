from __future__ import annotations

import os
from dotenv import load_dotenv

from weekly_summary.extract.sellercloud.sellercloud_client import SellercloudClient, SellercloudConfig
from weekly_summary.extract.sellercloud.sc_run import pull_warehouse_inventory_qty

from weekly_summary.extract.amazon.pull_restock_inventory import pull_restock_inventory_raw
from weekly_summary.transform.restock_inventory import load_and_normalize_restock
from weekly_summary.transform.current_stock import compute_current_stock
from weekly_summary.helpers.asin_sku_mapping import load_asin_sku_mapping


load_dotenv(override=True)

# 1) Pull Amazon data
pulled = pull_restock_inventory_raw(reuse_if_exists=True)
df = load_and_normalize_restock(pulled.raw_path)
df2 = compute_current_stock(df)

# 2) Add SKU mapping
mapping = load_asin_sku_mapping()
df3 = (
    df2.merge(
        mapping[["ASIN", "SKU"]],
        left_on="asin",
        right_on="ASIN",
        how="left",
    )
    .rename(columns={"SKU": "sku", "ASIN": "asin_from_sheet"})
)

print(f"Amazon rows with SKU: {len(df3[df3['sku'].notna()])}")
print(f"Amazon rows WITHOUT SKU: {len(df3[df3['sku'].isna()])}")
print("\nFirst 10 Amazon SKUs:")
print(df3[['asin', 'sku']].head(10).to_string(index=False))

# 3) Pull Sellercloud data
sc_cfg = SellercloudConfig(
    rest_api_base_url=os.environ["SELLERCLOUD_REST_API_BASE_URL"],
    username=os.environ["SELLERCLOUD_USERNAME"],
    password=os.environ["SELLERCLOUD_PASSWORD"],
)
sc_client = SellercloudClient(sc_cfg)

sc_df = pull_warehouse_inventory_qty(
    sc_client,
    company_id=177,
    warehouse_id=142,
    page_size=200,
    exclude_zero_inventory=True,
    qty_field="InventoryAvailableQty",
    debug_first_item=False,
)

print(f"\n\nSellercloud rows: {len(sc_df)}")
print("\nFirst 10 Sellercloud SKUs:")
print(sc_df.head(10).to_string(index=False))

# 4) Try the merge
df_final = df3.merge(sc_df, on="sku", how="left")
print(f"\n\nAfter merge:")
print(f"Rows with sc_warehouse_qty: {len(df_final[df_final['sc_warehouse_qty'].notna()])}")
print(f"Rows WITHOUT sc_warehouse_qty: {len(df_final[df_final['sc_warehouse_qty'].isna()])}")

# Check for any matches
matches = df_final[df_final['sc_warehouse_qty'].notna()]
if len(matches) > 0:
    print("\nMatched rows (first 5):")
    print(matches[['sku', 'asin', 'sc_warehouse_qty']].head(5).to_string(index=False))
else:
    print("\nNO MATCHES! Checking if SKUs are in both datasets...")
    amazon_skus = set(df3['sku'].dropna().unique())
    sc_skus = set(sc_df['sku'].unique())
    print(f"\nAmazon SKUs: {len(amazon_skus)}")
    print(f"Sellercloud SKUs: {len(sc_skus)}")
    print(f"\nCommon SKUs: {len(amazon_skus & sc_skus)}")
    print(f"\nSample Amazon SKUs: {list(amazon_skus)[:5]}")
    print(f"Sample SC SKUs: {list(sc_skus)[:5]}")