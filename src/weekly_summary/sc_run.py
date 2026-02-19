from __future__ import annotations

from dotenv import load_dotenv

from weekly_summary.extract.sellercloud.sellercloud_extract import get_inventory_data
from weekly_summary.extract.amazon.pull_restock_inventory import pull_restock_inventory_raw
from weekly_summary.transform.restock_inventory import load_and_normalize_restock
from weekly_summary.transform.current_stock import compute_current_stock
from weekly_summary.helpers.asin_sku_mapping import load_asin_sku_mapping
import pandas as pd

WELLES_INVENTORY_COL = "190-welles inventory"


def pull_warehouse_inventory_qty(
    *,
    saved_view_id: int = 187,  # Saved view 187: inventory report for 190 Welles warehouse
    warehouse_id: int = 142,   # Warehouse 142: 190 Welles - Bins
) -> pd.DataFrame:
    """
    Fetch real-time warehouse inventory from Sellercloud Delta API.

    Returns a DataFrame with columns ["sku", "190-welles inventory"].
    """
    items = get_inventory_data(saved_view_id=saved_view_id, warehouse_id=warehouse_id)

    if not items:
        return pd.DataFrame(columns=["sku", WELLES_INVENTORY_COL])

    rows = [{"sku": item.sku, WELLES_INVENTORY_COL: item.available} for item in items]
    df = pd.DataFrame(rows)

    # Group by SKU and take max available quantity
    df = (
        df.groupby("sku", as_index=False)[WELLES_INVENTORY_COL]
        .max()
        .sort_values(WELLES_INVENTORY_COL, ascending=False)
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

    # 5) Sellercloud: pull real-time warehouse inventory from Delta API
    print("\nPulling inventory from Sellercloud Delta API...")
    sc_df = pull_warehouse_inventory_qty()
    print(f"Sellercloud rows: {len(sc_df)}")
    print("\nSellercloud SKUs returned:")
    print(sc_df[["sku", WELLES_INVENTORY_COL]].to_string(index=False))

    # 6) Merge Sellercloud qty into the restock-derived data on SKU
    print("\n\nAmazon SKUs (unique):")
    print(df3[["sku"]].drop_duplicates().head(20).to_string(index=False))

    df_final = df3.merge(sc_df, on="sku", how="left")
    df_final[WELLES_INVENTORY_COL] = df_final[WELLES_INVENTORY_COL].fillna(0.0)

    print(f"\nMatched rows: {(df_final[WELLES_INVENTORY_COL] > 0).sum()}")

    # 7) Display results: Amazon columns + Sellercloud available qty
    output_cols = [
        "sku",
        "asin",
        "inventory_available",
        "fc_transfer",
        "fc_processing",
        "inbound",
        "current_stock_per_6",
        WELLES_INVENTORY_COL,
    ]
    output_cols = [c for c in output_cols if c in df_final.columns]

    print("\n=== FINAL REPORT: Amazon + Sellercloud Inventory ===")
    print(df_final[output_cols].sort_values(WELLES_INVENTORY_COL, ascending=False).head(50).to_string(index=False))

    print("\nweekly_summary.run: done")


if __name__ == "__main__":
    main()