from __future__ import annotations

import os
import pandas as pd

from weekly_summary.extract.google_sheets import build_sheets_service, read_range
from weekly_summary.transform.gsheets_to_df import values_to_dataframe  # keep your current filename
from weekly_summary.transform.gross_net_clean import clean_gross_net_df
from weekly_summary.transform.gross_net_select import select_gross_net_mapping


SPREADSHEET_ID = "1lJvclI4hgSYRpBTmVMJjQLfKmLzD7xCDqD_BuzYEt-0"
RANGE_NAME = "AMZ US!A1:P"  # adjust if needed


def load_asin_sku_mapping(
    spreadsheet_id: str = SPREADSHEET_ID,
    range_name: str = RANGE_NAME,
) -> pd.DataFrame:
    """
    Returns a dataframe with columns: ASIN, SKU
    Pulled from your Gross&Net Google Sheet.
    """
    sa_path = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    service = build_sheets_service(sa_path)

    values = read_range(service, spreadsheet_id, range_name)

    # Drop leading empty rows so header parsing doesn't fail
    while values and (not values[0] or all(str(x).strip() == "" for x in values[0])):
        values = values[1:]

    df_raw = values_to_dataframe(values)
    df_clean = clean_gross_net_df(df_raw)
    mapping = select_gross_net_mapping(df_clean)

    # Normalize to exactly ASIN + SKU columns
    mapping = mapping[["ASIN", "SKU"]].copy()
    mapping["ASIN"] = mapping["ASIN"].astype(str).str.strip()
    mapping["SKU"] = mapping["SKU"].astype(str).str.strip()

    return mapping
