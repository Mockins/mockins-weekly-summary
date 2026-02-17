from __future__ import annotations

import pandas as pd

class MasterCartonMappingError(Exception):
    """Raised when master-carton quantity cannot be attached."""
    pass

def attach_master_carton_qty(
        gross_net: pd.DataFrame,
        master_carton: pd.DataFrame,
) -> pd.DataFrame: 
    """
    Attach master-carton quantity to the gross_net dataframe.
    
    Parameters
    ----------
    gross_net: DataFrame
        Cleaned gross & net dataframe (keyed by Mini SKU)
    master_carton: DataFrame
        Master carton mapping (Mini SKU -> Qty per Master)
        
        
    Returns
    -------
    DataFrame 
        gross_net with an added 'Qty per Master' column
    """
    # Normalize column names (Google Sheets headers sometimes have trailing ":" or extra spaces)
    gross_net = gross_net.rename(columns=lambda c: str(c).strip().rstrip(":"))
    master_carton = master_carton.rename(columns=lambda c: str(c).strip().rstrip(":"))

    required_gross = {"Mini SKU"}
    required_master = {"Mini SKU", "Qty per Master"}

    missing_gross = required_gross - set(gross_net.columns)
    missing_master = required_master - set(master_carton.columns)

    if missing_gross:
        raise MasterCartonMappingError(f"gross_net missing required columns: {sorted(missing_gross)}")
    if missing_master:
        raise MasterCartonMappingError(f"master_carton missing required columns: {sorted(missing_master)}")
    
    # Check for duplicate Mini SKU in master_carton
    if master_carton["Mini SKU"].duplicated().any():
        raise MasterCartonMappingError("duplicate Mini SKU found in master_carton")
    
    # Build a clean mapping DF with just the two columns we need
    mc = master_carton[["Mini SKU", "Qty per Master"]].copy()

    # Normalize Mini SKU values (strip spaces)
    mc["Mini SKU"] = mc["Mini SKU"].astype(str).str.strip()
    gross_net["Mini SKU"] = gross_net["Mini SKU"].astype(str).str.strip()

    # Rename for the output column name we want
    mc = mc.rename(columns={"Qty per Master": "qty_per_master"})

    # Merge onto gross_net
    out = gross_net.merge(mc, on="Mini SKU", how="left")

    return out