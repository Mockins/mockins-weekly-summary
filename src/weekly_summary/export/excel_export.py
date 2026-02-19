"""
Export full weekly inventory report to Excel.

Combines:
- Amazon restock data (asin, inventory_available, fc_transfer, etc.)
- SKU mapping from ASIN
- Sellercloud warehouse inventory from Delta API (190-welles inventory)

Handles missing SKUs by filling with NA.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from weekly_summary.extract.amazon.pull_restock_inventory import pull_restock_inventory_raw
from weekly_summary.helpers.asin_sku_mapping import load_asin_sku_mapping
from weekly_summary.sc_run import pull_warehouse_inventory_qty
from weekly_summary.transform.current_stock import compute_current_stock
from weekly_summary.transform.restock_inventory import load_and_normalize_restock

logger = logging.getLogger(__name__)


def generate_full_inventory_report(
    output_path: str = "data/weekly_summary.xlsx",
    reuse_amazon_cache: bool = True,
) -> str:
    """
    Generate complete inventory report with all SKUs from all sources.
    
    Combines:
    1. Amazon restock data (inventory_available, fc_transfer, etc.)
    2. Sellercloud warehouse inventory via Delta API (190-welles inventory)
    
    Any SKU missing from either source gets 'NA' for those columns.
    
    Args:
        output_path: Path to save Excel file
        reuse_amazon_cache: Reuse cached Amazon data if exists
        
    Returns:
        Path to created Excel file
    """
    load_dotenv(override=True)
    
    logger.info("Starting full inventory report generation...")
    
    # ============ 1. AMAZON DATA ============
    logger.info("Step 1: Pulling Amazon restock data...")
    pulled = pull_restock_inventory_raw(reuse_if_exists=reuse_amazon_cache)
    
    df_amazon = load_and_normalize_restock(pulled.raw_path)
    logger.info(f"  Amazon rows: {len(df_amazon)}")
    
    # Compute additional columns
    df_amazon = compute_current_stock(df_amazon)
    
    # ============ 2. ADD SKU VIA ASIN MAPPING ============
    logger.info("Step 2: Mapping ASIN to SKU...")
    mapping = load_asin_sku_mapping()
    
    df_with_sku = df_amazon.merge(
        mapping[["ASIN", "SKU"]],
        left_on="asin",
        right_on="ASIN",
        how="left",
    ).rename(columns={"SKU": "sku"})
    
    logger.info(f"  After SKU mapping: {len(df_with_sku)}")
    
    # ============ 3. SELLERCLOUD DATA (DELTA API) ============
    logger.info("Step 3: Pulling Sellercloud warehouse inventory from Delta API...")
    df_sc = pull_warehouse_inventory_qty(
        saved_view_id=187,  # Inventory report for 190 Welles
        warehouse_id=142,   # 190 Welles - Bins warehouse
    )
    
    logger.info(f"  Sellercloud rows: {len(df_sc)}")
    
    # ============ 4. MERGE ALL DATA ============
    logger.info("Step 4: Merging all data sources...")
    
    # Merge Amazon + Sellercloud on SKU (full outer join to keep all SKUs)
    df_merged = df_with_sku.merge(
        df_sc,
        on="sku",
        how="outer",  # Keep all SKUs from both sources
    )
    
    logger.info(f"  After merge: {len(df_merged)} total SKUs")
    
    # ============ 5. SELECT AND ORDER COLUMNS ============
    output_cols = [
        "sku",
        "asin",
        "inventory_available",
        "fc_transfer",
        "fc_processing",
        "inbound",
        "current_stock_per_6",
        "190-welles inventory",
    ]
    
    # Only include columns that exist
    output_cols = [c for c in output_cols if c in df_merged.columns]
    
    df_final = df_merged[output_cols].copy()
    
    # Fill missing values with 'NA'
    df_final = df_final.fillna("NA")
    
    # Sort by 190-welles inventory descending (or by SKU if column doesn't exist)
    if "190-welles inventory" in df_final.columns:
        # Convert to numeric for sorting, keeping NAs at bottom
        df_final["_sort_key"] = pd.to_numeric(
            df_final["190-welles inventory"], 
            errors="coerce"
        )
        df_final = df_final.sort_values("_sort_key", ascending=False, na_position="last")
        df_final = df_final.drop("_sort_key", axis=1)
    else:
        df_final = df_final.sort_values("sku")
    
    # ============ 6. EXPORT TO EXCEL ============
    logger.info("Step 5: Exporting to Excel...")
    
    # Create output directory if it doesn't exist
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write to Excel with formatting
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df_final.to_excel(
            writer,
            sheet_name="Inventory Summary",
            index=False,
            freeze_panes=(1, 0),
        )
        
        # Auto-adjust column widths
        worksheet = writer.sheets["Inventory Summary"]
        for column in worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except TypeError:
                    pass
            
            adjusted_width = min(max_length + 2, 50)
            worksheet.column_dimensions[column_letter].width = adjusted_width
    
    logger.info(f"✓ Report exported: {output_path}")
    logger.info(f"  Total SKUs: {len(df_final)}")
    logger.info(f"  Columns: {', '.join(output_cols)}")
    
    print(f"\n✅ Report saved to: {output_path}")
    print(f"   Total SKUs: {len(df_final)}")
    
    return str(output_path)


def main() -> None:
    """Generate and export full inventory report."""
    logging.basicConfig(level=logging.INFO)
    
    output_file = generate_full_inventory_report(
        output_path="data/weekly_summary.xlsx",
        reuse_amazon_cache=True,
    )
    
    print(f"\n📊 Report ready: {output_file}")


if __name__ == "__main__":
    main()