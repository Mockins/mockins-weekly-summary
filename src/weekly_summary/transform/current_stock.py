from __future__ import annotations

import pandas as pd


def compute_current_stock(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds:
      - inbound (single column)
      - current_stock
      - current_stock_per_6

    Logic (aligned to your weekly sheet intent):
      current_stock = inventory_available + fc_transfer + fc_processing + inbound
      current_stock_per_6 = current_stock / 6
    """
    out = df.copy()

    # Required base columns
    required = ["inventory_available", "fc_transfer", "fc_processing"]
    missing = [c for c in required if c not in out.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Build a single inbound column:
    # - If restock parser already produced "inbound", use it
    # - Else, if it produced inbound_working/shipped/receiving, sum them
    if "inbound" in out.columns:
        inbound_series = out["inbound"]
    else:
        parts = [c for c in ["inbound_working", "inbound_shipped", "inbound_receiving"] if c in out.columns]
        if parts:
            inbound_series = out[parts].fillna(0).sum(axis=1)
        else:
            # If inbound doesn't exist at all, treat as 0
            inbound_series = 0

    out["inbound"] = pd.to_numeric(inbound_series, errors="coerce").fillna(0)

    out["current_stock"] = (
        pd.to_numeric(out["inventory_available"], errors="coerce").fillna(0)
        + pd.to_numeric(out["fc_transfer"], errors="coerce").fillna(0)
        + pd.to_numeric(out["fc_processing"], errors="coerce").fillna(0)
        + out["inbound"]
    )

    out["current_stock_per_6"] = out["current_stock"] / 6
    return out
