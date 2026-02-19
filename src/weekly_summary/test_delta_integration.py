"""Test the Delta API integration."""

from __future__ import annotations

import logging

from weekly_summary.extract.sellercloud import (
    get_inventory_data,
    get_inventory_by_sku,
    get_inventory_totals,
)

logging.basicConfig(level=logging.INFO)


def main() -> None:
    print("=== Sellercloud Delta API Integration Test ===\n")
    
    # Get totals
    print("Getting inventory totals...")
    totals = get_inventory_totals()
    print(f"  Total Physical: {totals['total_physical']}")
    print(f"  Total Available: {totals['total_available']}")
    print(f"  Total Value: ${totals['total_value']:,.2f}\n")
    
    # Get all inventory
    print("Getting all inventory...")
    items = get_inventory_data()
    print(f"  Total items: {len(items)}\n")
    
    # Search for specific SKU
    print("Searching for NG-2Z-DRP-AM-48P-42...")
    item = get_inventory_by_sku("NG-2Z-DRP-AM-48P-42")
    if item:
        print(f"  ✓ Found!")
        print(f"    SKU: {item.sku}")
        print(f"    Product: {item.product_name}")
        print(f"    Warehouse: {item.warehouse}")
        print(f"    Physical: {item.physical} | Reserved: {item.reserved} | Available: {item.available}")
        print(f"    Value: ${item.value:,.2f}")
    else:
        print(f"  ✗ Not found")
    
    print("\n=== Test Complete ===")


if __name__ == "__main__":
    main()