"""
Main report generator that orchestrates the full weekly summary pipeline.

Combines Amazon restock data with Sellercloud warehouse inventory.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from weekly_summary.export.excel_export import generate_full_inventory_report

logger = logging.getLogger(__name__)


class WeeklySummaryReportGenerator:
    """Generate complete weekly summary reports with all data sources."""
    
    def __init__(
        self,
        output_dir: str = "data",
        reuse_amazon_cache: bool = True,
    ):
        """
        Initialize report generator.
        
        Args:
            output_dir: Directory to save reports
            reuse_amazon_cache: Reuse cached Amazon data if exists
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.reuse_amazon_cache = reuse_amazon_cache
        
        load_dotenv(override=True)
    
    def generate(self) -> str:
        """
        Generate complete weekly summary report.
        
        Pipeline:
        1. Pull Amazon restock data
        2. Map ASIN -> SKU
        3. Compute current stock metrics
        4. Pull Sellercloud warehouse inventory (Delta API)
        5. Merge all data (full outer join)
        6. Export to Excel with all SKUs
        
        Returns:
            Path to generated Excel file
        """
        logger.info("=" * 60)
        logger.info("WEEKLY SUMMARY REPORT GENERATOR")
        logger.info("=" * 60)
        
        output_file = self.output_dir / "weekly_summary.xlsx"
        
        # Generate report using the excel_export module
        result = generate_full_inventory_report(
            output_path=str(output_file),
            reuse_amazon_cache=self.reuse_amazon_cache,
        )
        
        logger.info("=" * 60)
        logger.info("✅ REPORT GENERATION COMPLETE")
        logger.info("=" * 60)
        
        return result
    
    def generate_with_output(self) -> dict[str, str]:
        """
        Generate report and return detailed output info.
        
        Returns:
            Dict with:
            - output_file: Path to Excel file
            - rows: Number of SKUs in report
        """
        output_file = self.generate()
        
        # Read back to get row count
        df = pd.read_excel(output_file)
        
        return {
            "output_file": output_file,
            "rows": len(df),
            "columns": list(df.columns),
        }


def main() -> None:
    """Generate weekly summary report and save to Excel."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    
    generator = WeeklySummaryReportGenerator(
        output_dir="data",
        reuse_amazon_cache=True,
    )
    
    output_info = generator.generate_with_output()
    
    print("\n" + "=" * 60)
    print("📊 REPORT SUMMARY")
    print("=" * 60)
    print(f"Output File: {output_info['output_file']}")
    print(f"Total SKUs:  {output_info['rows']}")
    print(f"Columns:     {', '.join(output_info['columns'])}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()