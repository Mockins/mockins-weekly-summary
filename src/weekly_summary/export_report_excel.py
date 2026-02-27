from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv

from weekly_summary.export_to_excel import export_report_to_excel
from weekly_summary.extract.amazon.pull_restock_inventory import pull_restock_inventory_raw
from weekly_summary.extract.sellercloud.pull_inventory_by_view import pull_190_welles_inventory
from weekly_summary.helpers.asin_sku_mapping import load_asin_sku_mapping
from weekly_summary.transform.current_stock import compute_current_stock
from weekly_summary.transform.restock_inventory import load_and_normalize_restock
from weekly_summary.transform.sales_windows import compute_sku_sales_windows


def build_report_dataframe(*, reuse_cache: bool = False):
    """
    Rebuilds the same df_final as run.py, without changing run.py.
    Returns (df_final, output_cols, end_date).
    """
    load_dotenv(override=True)
    print("weekly_summary.export_report_excel: starting")

    pulled = pull_restock_inventory_raw(reuse_if_exists=True)
    print(f"Using restock raw: {pulled.raw_path}")

    df_restock = load_and_normalize_restock(pulled.raw_path)
    print("Restock normalized rows:", len(df_restock))

    df_amz = compute_current_stock(df_restock)

    mapping = load_asin_sku_mapping()
    print("ASIN->SKU mapping rows:", len(mapping))

    df_amz = (
        df_amz.merge(
            mapping[["ASIN", "SKU"]],
            left_on="asin",
            right_on="ASIN",
            how="left",
        )
        .rename(columns={"SKU": "sku"})
        .drop(columns=["ASIN"])
    )
    df_amz["sku"] = df_amz["sku"].astype(str).str.strip()
    df_amz["asin"] = df_amz["asin"].astype(str).str.strip()

    print("Amazon + SKU mapping rows:", len(df_amz))

    print("\nPulling inventory from SellerCloud (Inventory/GetAllByView)...")

    server_id = os.getenv("SELLERCLOUD_SERVER_ID") or os.getenv("SELLERCLOUD_SERVER")
    username = os.getenv("SELLERCLOUD_USERNAME")
    password = os.getenv("SELLERCLOUD_PASSWORD")
    if not server_id or not username or not password:
        raise RuntimeError(
            "Missing SellerCloud env vars. Need SELLERCLOUD_SERVER_ID (or SELLERCLOUD_SERVER), "
            "SELLERCLOUD_USERNAME, SELLERCLOUD_PASSWORD"
        )

    sc_df = pull_190_welles_inventory(
        server_id=server_id,
        username=username,
        password=password,
        view_id=187,
    )
    sc_df = sc_df.rename(columns={"SKU": "sku", "Welles190Qty": "190-welles inventory"}).copy()
    sc_df["sku"] = sc_df["sku"].astype(str).str.strip()
    print("SellerCloud rows:", len(sc_df))

    df_final = df_amz.merge(sc_df, on="sku", how="left")
    if "190-welles inventory" in df_final.columns:
        df_final["190-welles inventory"] = df_final["190-welles inventory"].fillna(0.0)

    print("\nComputing Amazon Sales & Traffic windows (Units Ordered) with window caching...")

    end_date = date.today() - timedelta(days=1)
    db_path = Path("data") / "cache" / "spapi_reports.sqlite"

    df_sales_windows = compute_sku_sales_windows(
        end_date=end_date,
        asin_sku_map=mapping[["ASIN", "SKU"]],
        db_path=db_path,
        reuse_cache=reuse_cache,
    )

    # Merge on both sku+asin so base vs LOC stay separate; outer keeps sales-only LOC rows
    df_sales_windows["sku"] = df_sales_windows["sku"].astype(str).str.strip()
    df_sales_windows["asin"] = df_sales_windows["asin"].astype(str).str.strip()

    df_final["sku"] = df_final["sku"].astype(str).str.strip()
    df_final["asin"] = df_final["asin"].astype(str).str.strip()

    df_final = df_final.merge(df_sales_windows, on=["sku", "asin"], how="outer")

    # Fill numeric columns introduced by outer merge
    window_cols = [
        "1 Day",
        "7 Days",
        "8-14",
        "15-21",
        "22-28",
        "1-28",
        "29-56",
        "57-84",
        "4 Week Avg",
        "3 Month Avg",
    ]
    for c in window_cols:
        if c in df_final.columns:
            df_final[c] = df_final[c].fillna(0)

    for c in [
        "inventory_available",
        "fc_transfer",
        "fc_processing",
        "inbound",
        "current_stock_per_6",
        "190-welles inventory",
    ]:
        if c in df_final.columns:
            df_final[c] = df_final[c].fillna(0)

    output_cols = [
        "sku",
        "asin",
        "inventory_available",
        "fc_transfer",
        "fc_processing",
        "inbound",
        "current_stock_per_6",
        "190-welles inventory",
        "1 Day",
        "7 Days",
        "8-14",
        "15-21",
        "22-28",
        "1-28",
        "29-56",
        "57-84",
        "4 Week Avg",
        "3 Month Avg",
    ]
    output_cols = [c for c in output_cols if c in df_final.columns]

    return df_final, output_cols, end_date


def main() -> None:
    df_final, output_cols, end_date = build_report_dataframe(reuse_cache=False)

    res = export_report_to_excel(
        df_final[output_cols],
        base_filename=f"weekly_summary_report_{end_date.isoformat()}",
        include_loc_sheet=True,
    )

    print("\n=== EXCEL EXPORT ===")
    print("Path:", res.path)
    print("Total rows:", res.total_rows)
    print("LOC rows:", res.loc_rows)


if __name__ == "__main__":
    main()