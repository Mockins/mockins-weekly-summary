from pathlib import Path

import pandas as pd

from weekly_summary.transform.restock_inventory import load_and_normalize_restock


def test_restock_inventory_normalization():
    # Find today's cached raw file
    base = Path("data/raw/amazon/restock_inventory")
    assert base.exists(), "Restock cache directory missing"

    # Pick latest folder
    date_dirs = sorted([p for p in base.iterdir() if p.is_dir()])
    assert date_dirs, "No restock date folders found"

    latest = date_dirs[-1]
    raw_files = list(latest.glob("restock_inventory_raw_*"))
    assert raw_files, "No restock raw file found"

    raw_path = raw_files[0]

    df = load_and_normalize_restock(raw_path)

    # Basic structural checks
    assert not df.empty
    assert "sku" in df.columns
    assert "inventory_available" in df.columns
    assert "fc_transfer" in df.columns
    assert "fc_processing" in df.columns
    assert "inbound" in df.columns

    # 1 row per sku
    assert df["sku"].is_unique

    # numeric types
    for col in [
        "inventory_available",
        "fc_transfer",
        "fc_processing",
        "inbound",
    ]:
        assert pd.api.types.is_numeric_dtype(df[col])
