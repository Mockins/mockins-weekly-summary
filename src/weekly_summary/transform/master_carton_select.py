from __future__ import annotations

import pandas as pd

class MasterCartonParseError(ValueError):
    """Raised when the Master Carton sheet cannot be parsed into a mapping."""

def select_master_carton_mapping(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a clean mapping: 
        Mini SKU -> Qty per Master
    
    Expected source columns (from Google Sheets) are often: 
        - "Mini SKU:" (note the colon)
        - "Qty per Master: " (note the colon)
        
    Output columns (standardized, no colons):
        - "Mini SKU"
        - "Qty per Master"
        """
    required = ["Mini SKU:", "Qty per Master:"]
    missing = [c for c in required if c not in df.columns]
    if missing: 
        raise MasterCartonParseError(f"Missing required columns: {missing}")
    
    out = df[required].copy()

    # Normalize column names (remove trailing colons)
    out = out.rename(
        columns = {
            "Mini SKU:": "Mini SKU", 
            "Qty per Master:": "Qty per Master",   
        }
    )

    # Clean Mini SKU
    out["Mini SKU"] = out["Mini SKU"].astype(str).str.strip()
    out = out[out["Mini SKU"] != ""]  # drop blank

    # Clean Qty per Master (must be numeric integer-ish)
    out["Qty per Master"] = pd.to_numeric(out["Qty per Master"], errors="coerce")
    out = out.dropna(subset=["Qty per Master"])
    out["Qty per Master"] = out["Qty per Master"].astype(int)

    # De-dupe: if duplicates exist, keep the last non-null entry
    out = out.drop_duplicates(subset=["Mini SKU"], keep="last").reset_index(drop=True)

    return out 