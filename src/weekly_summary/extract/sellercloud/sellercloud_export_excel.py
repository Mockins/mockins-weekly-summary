"""
Export inventory data to Excel for comparison with SellerCloud report.
"""
import os
import logging
import sys
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
from weekly_summary.extract.sellercloud.pull_inventory_by_view import pull_190_welles_inventory

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Load environment variables from .env
load_dotenv()


def main():
    """Pull inventory and export to Excel."""
    
    # Load credentials from environment
    server_id = os.getenv("SERVER_ID")
    username = os.getenv("SELLERCLOUD_USERNAME")
    password = os.getenv("SELLERCLOUD_PASSWORD")
    
    if not all([server_id, username, password]):
        logger.error(
            "Missing required environment variables: "
            "SERVER_ID, SELLERCLOUD_USERNAME, SELLERCLOUD_PASSWORD"
        )
        sys.exit(1)
    
    try:
        logger.info("Pulling inventory from SellerCloud...")
        df = pull_190_welles_inventory(
            server_id=server_id,
            username=username,
            password=password,
            view_id=187,
            page_size=50,
        )
        
        # Create output filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"Welles190_Inventory_{timestamp}.xlsx"
        
        logger.info(f"\nExporting {len(df)} SKUs to {output_file}...")
        
        # Create Excel writer
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Write main data
            df.to_excel(writer, sheet_name='Inventory', index=False)
            
            # Get the worksheet to format it
            worksheet = writer.sheets['Inventory']
            
            # Auto-adjust column widths
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
            
            # Add summary sheet
            summary_df = pd.DataFrame({
                'Metric': [
                    'Total SKUs',
                    'Total Quantity',
                    'Min Quantity',
                    'Max Quantity',
                    'Average Quantity',
                    'Median Quantity',
                    'Export Date',
                ],
                'Value': [
                    len(df),
                    f"{df['Welles190Qty'].sum():,}",
                    df['Welles190Qty'].min(),
                    df['Welles190Qty'].max(),
                    f"{df['Welles190Qty'].mean():.2f}",
                    f"{df['Welles190Qty'].median():.2f}",
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ]
            })
            
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Format summary sheet
            summary_worksheet = writer.sheets['Summary']
            for column in summary_worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                summary_worksheet.column_dimensions[column_letter].width = adjusted_width
        
        logger.info(f"\n{'='*70}")
        logger.info(f"✅ Export successful!")
        logger.info(f"{'='*70}")
        logger.info(f"File: {output_file}")
        logger.info(f"Total SKUs: {len(df)}")
        logger.info(f"Total Quantity: {df['Welles190Qty'].sum():,}")
        logger.info(f"\nNow compare this file with the SellerCloud report to find")
        logger.info(f"which SKUs are in this export but NOT in the SellerCloud report.")
        logger.info(f"{'='*70}\n")
        
        return 0
        
    except Exception as e:
        logger.error(f"Failed to export inventory: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())