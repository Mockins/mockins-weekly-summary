from __future__ import annotations

from dataclasses import dataclass
from typing import List

import pandas as pd


class GSheetsParseError(ValueError):
    pass

def slice_values_from_header(values: List[List[str]], required_header: str) -> List[List[str]]:
    """
    Find the first row containing `required_header` (case-insensitive) and return values starting there.
    Raises GSheetsParseError if not found.

    This is used for messy human-formatted sheets where the real header row is not the first row.
    """
    needle = required_header.strip().lower()
    for idx, row in enumerate(values):
        if not row:
            continue
        lowered = [str(cell).strip().lower() for cell in row if cell is not None]
        if needle in lowered:
            return values[idx:]
    raise GSheetsParseError(f"Required header '{required_header}' not found.")


def values_to_dataframe(values: List[List[str]]) -> pd.DataFrame:
    """
    Convert Google Sheets API `values` (list of rows) into a pandas DataFrame.

    Robust behavior:
    - Requires at least one row (header).
    - Drops columns whose header cell is blank/whitespace.
    - Pads short rows so DataFrame is rectangular.
    - Ensures remaining column names are unique.
    """
    if not values:
        raise GSheetsParseError("No values provided.")

    header = values[0]
    if not header or all(str(h).strip() == "" for h in header):
        raise GSheetsParseError("Header row is empty.")

    # Normalize header cells to strings
    header_str = ["" if h is None else str(h) for h in header]

    # Keep only columns with non-empty names
    keep_idx = [i for i, h in enumerate(header_str) if h.strip() != ""]
    if not keep_idx:
        raise GSheetsParseError("Header contains no usable (non-empty) column names.")

    header_kept = [header_str[i].strip() for i in keep_idx]

    # Build kept data rows (drop the same columns)
    data_rows = []
    for row in values[1:]:
        row = [] if row is None else row
        # pad row to header length so indexing is safe
        padded = list(row) + [""] * (len(header_str) - len(row))
        data_rows.append([padded[i] for i in keep_idx])

    df = pd.DataFrame(data_rows, columns=header_kept)

    # Ensure unique column names (avoid duplicates after stripping)
    cols = list(df.columns)
    seen = {}
    new_cols = []
    for c in cols:
        base = c
        if base not in seen:
            seen[base] = 0
            new_cols.append(base)
        else:
            seen[base] += 1
            new_cols.append(f"{base}__{seen[base]}")
    df.columns = new_cols

    return df
