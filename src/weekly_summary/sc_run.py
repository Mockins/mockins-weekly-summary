"""
Sellercloud inventory extraction using SellerCloud REST API (token-based).
Used by both run.py and report_generator.
"""

from __future__ import annotations

import os
from dotenv import load_dotenv
import pandas as pd

from weekly_summary.extract.sellercloud.pull_inventory_by_view import pull_190_welles_inventory

WELLES_INVENTORY_COL = "190-welles inventory"


def pull_warehouse_inventory_qty(
    *,
    saved_view_id: int = 187,  # Saved view 187: inventory report for 190 Welles warehouse
) -> pd.DataFrame:
    """
    Fetch real-time warehouse inventory from SellerCloud using the saved view endpoint:
      GET Inventory/GetAllByView

    Returns a DataFrame with columns ["sku", "190-welles inventory"].
    """
    load_dotenv(override=True)

    server_id = os.getenv("SELLERCLOUD_SERVER")
    username = os.getenv("SELLERCLOUD_USERNAME")
    password = os.getenv("SELLERCLOUD_PASSWORD")

    missing = [k for k, v in {
        "SELLERCLOUD_SERVER": server_id,
        "SELLERCLOUD_USERNAME": username,
        "SELLERCLOUD_PASSWORD": password,
    }.items() if not v or str(v).strip() == ""]

    if missing:
        raise RuntimeError(f"Missing required SellerCloud env vars: {missing}")

    df_raw = pull_190_welles_inventory(
        server_id=str(server_id).strip(),
        username=str(username).strip(),
        password=str(password),
        view_id=int(saved_view_id),
        page_size=50,
    )

    if df_raw.empty:
        return pd.DataFrame(columns=["sku", WELLES_INVENTORY_COL])

    # Normalize to the column names run.py expects
    # pull_190_welles_inventory returns: ["SKU", "Welles190Qty"]
    out = pd.DataFrame(
        {
            "sku": df_raw["SKU"].astype(str).str.strip(),
            WELLES_INVENTORY_COL: pd.to_numeric(df_raw["Welles190Qty"], errors="coerce").fillna(0.0),
        }
    )

    # If duplicates exist for any reason, take max
    out = (
        out.groupby("sku", as_index=False)[WELLES_INVENTORY_COL]
        .max()
        .sort_values(WELLES_INVENTORY_COL, ascending=False)
        .reset_index(drop=True)
    )

    return out


def main() -> None:
    """Quick test of SellerCloud inventory pull via saved view endpoint."""
    load_dotenv(override=True)
    print("sc_run: Fetching SellerCloud inventory via Inventory/GetAllByView...")

    df = pull_warehouse_inventory_qty()

    print(f"\n✓ Got {len(df)} SKUs from SellerCloud")
    print("\nTop 20 SKUs by available quantity:")
    print(df.head(20).to_string(index=False))

    print("\nsc_run: done")


if __name__ == "__main__":
    main()