from __future__ import annotations
from dataclasses import dataclass
import pandas as pd

@dataclass(frozen=True)
class GrossNetSchema:
    sku_col: str = "SKU"
    asin_col: str = "ASIN"
    price_col: str = "Selling Price"
    cost_col: str = "Cost"
    freight_packaging_col: str = "Freight Cost / Packaging"

MONEY_COLS_DEFAULT = [
    "Selling Price", 
    "Price Before PD", 
    "Cost", 
    "Freight Cost / Packaging", 
    "FBA commision", 
    "Pick and Pack", 
    "FBA Fee (Commission + Pick and Pack)", 
    "Placement Service Fee",
]

def _to_numeric_money(series: pd.Series) -> pd.Series:
    """
    Convert a money-like series (e.g. '$1,234.56', '12.00') to float. 
    Blanks become NA. 
    """
    s = series.astype("string").str.strip()

    # Remove common currency formatting
    s = s.str.replace("$", "", regex=False).str.replace(",", "", regex=False)

    # Treat empty strings as missing
    s = s.replace("", pd.NA)

    return pd.to_numeric(s, errors="coerce")


def clean_gross_net_df(df: pd.DataFrame, money_cols: list[str] | None = None) -> pd.DataFrame:
    """
    Return a cleaned copy of the Gross & Net DataFrame:
    - trims whitespace on all string columns
    - converts money columns to numeric floats
    """

    out = df.copy()

    # Strip whitespace from all object/string columns
    for col in out.columns: 
        if pd.api.types.is_object_dtype(out[col]) or pd.api.types.is_string_dtype(out[col]):
            out[col] = out[col].astype("string").str.strip()

    cols = money_cols if money_cols is not None else MONEY_COLS_DEFAULT

    for col in cols: 
        if col in out.columns:
            out[col] = _to_numeric_money(out[col])

    return out
