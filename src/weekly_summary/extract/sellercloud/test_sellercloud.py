"""
Test script for SellerCloud inventory extraction.
Prints results and summary statistics.
"""
import os
import logging
import sys
from dotenv import load_dotenv
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
    """Test the SellerCloud inventory pull."""
    
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
        logger.info("Starting inventory pull from Monday Inventory Report (View 187)...")
        df = pull_190_welles_inventory(
            server_id=server_id,
            username=username,
            password=password,
            view_id=187,
            page_size=50,
        )
        
        logger.info(f"\n{'='*70}")
        logger.info("FINAL RESULTS")
        logger.info(f"{'='*70}")
        logger.info(f"Total unique parent SKUs: {len(df)}")
        logger.info(f"Total inventory quantity: {df['Welles190Qty'].sum():,}")
        
        logger.info(f"\nFirst 20 rows:")
        logger.info(f"\n{df.head(20).to_string(index=False)}")
        logger.info(f"\nLast 20 rows:")
        logger.info(f"\n{df.tail(20).to_string(index=False)}")
        logger.info(f"\n{'='*70}")
        logger.info(f"Summary Statistics:")
        logger.info(f"  - Min quantity: {df['Welles190Qty'].min():,}")
        logger.info(f"  - Max quantity: {df['Welles190Qty'].max():,}")
        logger.info(f"  - Average quantity: {df['Welles190Qty'].mean():.2f}")
        logger.info(f"  - Median quantity: {df['Welles190Qty'].median():.2f}")
        logger.info(f"{'='*70}\n")
        
        # Compare to expected
        expected = 317
        difference = len(df) - expected
        logger.info(f"Expected SKUs: {expected}")
        logger.info(f"Actual SKUs: {len(df)}")
        logger.info(f"Difference: {difference} ({'MORE' if difference > 0 else 'FEWER'})")
        logger.info(f"\n{'='*70}\n")
        
        return 0
        
    except Exception as e:
        logger.error(f"Failed to pull inventory: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())