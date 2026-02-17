from __future__ import annotations

import pandas as pd

REQUIRED_COLS = ["SKU", "ASIN", "Mini SKU", "Selling Price"]

def select_gross_net_mapping(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reduce Gross & Net to the minimal mapping needed for weekly Summary:
    SKU -> ASIN, Mini SKU, Selling Price
    
    Returns a de-duplicated DataFrame keyed by SKU.
    Raises ValueError if required columns are missing.
    """
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing: 
        raise ValueError(f"Gross & Net missing required columns: {missing}")
    
    out = df[REQUIRED_COLS].copy()

    # Normalize SKU key
    out["SKU"] = out["SKU"].astype("string").str.strip()

    # Drop rows without SKU (cannot join)
    out = out[out["SKU"].notna() & (out["SKU"] != "")]

    # De-duplicate by SKU (keep the first occurrence)
    out = out.drop_duplicates(subset=["SKU"], keep="first").reset_index(drop=True)

    return out
