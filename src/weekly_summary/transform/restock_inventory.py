from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pandas as pd


@dataclass(frozen=True)
class RestockInventoryNormalized:
    df: pd.DataFrame
    source_path: Path
    delimiter: Literal[",", "\t"]


# We now require ASIN because we want to key everything by ASIN (more stable than SKU).
REQUIRED_INPUT_COLS = ["ASIN", "Merchant SKU", "Available", "FC transfer", "FC Processing", "Inbound"]
OPTIONAL_INBOUND_COLS = ["Working", "Shipped", "Receiving"]


def _detect_delimiter(sample: str) -> Literal[",", "\t"]:
    # Restock exports can be CSV or TSV depending on how Seller Central generated it.
    if "\t" in sample and sample.count("\t") >= sample.count(","):
        return "\t"
    return ","


def read_restock_raw(path: str | Path) -> RestockInventoryNormalized:
    path = Path(path)

    raw_bytes = path.read_bytes()

    # Try common encodings for Seller Central exports
    decoded_text: str | None = None
    for enc in ("utf-8-sig", "cp1252", "utf-8"):
        try:
            decoded_text = raw_bytes.decode(enc)
            break
        except UnicodeDecodeError:
            continue

    if decoded_text is None:
        decoded_text = raw_bytes.decode("utf-8", errors="replace")

    first_lines = "\n".join([ln for ln in decoded_text.splitlines() if ln.strip()][:5])
    delim = _detect_delimiter(first_lines)

    # Keep everything as string initially, then coerce numeric columns after renaming
    df = pd.read_csv(path, sep=delim, dtype=str, encoding="cp1252", low_memory=False)

    return RestockInventoryNormalized(df=df, source_path=path, delimiter=delim)


def normalize_restock_inventory(df_raw: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in REQUIRED_INPUT_COLS if c not in df_raw.columns]
    if missing:
        raise ValueError(
            f"Restock raw missing required columns: {missing}. Found: {list(df_raw.columns)}"
        )

    for c in OPTIONAL_INBOUND_COLS:
        if c not in df_raw.columns:
            df_raw[c] = None

    df = df_raw[REQUIRED_INPUT_COLS + OPTIONAL_INBOUND_COLS].copy()

    # Standardize column names
    df = df.rename(
        columns={
            "ASIN": "asin",
            "Merchant SKU": "merchant_sku",
            "Available": "inventory_available",
            "FC transfer": "fc_transfer",
            "FC Processing": "fc_processing",
            "Inbound": "inbound",
            "Working": "inbound_working",
            "Shipped": "inbound_shipped",
            "Receiving": "inbound_receiving",
        }
    )

    # Clean identifiers
    df["asin"] = df["asin"].astype(str).str.strip()
    df["merchant_sku"] = df["merchant_sku"].astype(str).str.strip()

    numeric_cols = [
        "inventory_available",
        "fc_transfer",
        "fc_processing",
        "inbound",
        "inbound_working",
        "inbound_shipped",
        "inbound_receiving",
    ]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    # Your sheet’s “current stock” definition (matches what you described):
    # inventory_available + fc_transfer + fc_processing + inbound
    df["current_stock"] = (
        df["inventory_available"] + df["fc_transfer"] + df["fc_processing"] + df["inbound"]
    )

    # Collapse to 1 row per ASIN (stable key). Multiple merchant_sku can map to one ASIN,
    # so we do not aggregate merchant_sku into the final table.
    df_asin = df.groupby("asin", as_index=False)[numeric_cols + ["current_stock"]].sum()

    # Validation helpers (optional)
    df_asin["inbound_alt"] = (
        df_asin["inbound_working"] + df_asin["inbound_shipped"] + df_asin["inbound_receiving"]
    )
    df_asin["inbound_diff"] = df_asin["inbound"] - df_asin["inbound_alt"]

    return df_asin


def load_and_normalize_restock(path: str | Path) -> pd.DataFrame:
    raw = read_restock_raw(path)
    return normalize_restock_inventory(raw.df)
