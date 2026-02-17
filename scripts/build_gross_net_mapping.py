import os

from dotenv import load_dotenv

from weekly_summary.extract.google_sheets import build_sheets_service, read_range
from weekly_summary.transform.gsheets_to_df import values_to_dataframe
from weekly_summary.transform.gross_net_clean import clean_gross_net_df
from weekly_summary.transform.gross_net_select import select_gross_net_mapping


SPREADSHEET_ID = "1lJvclI4hgSYRpBTmVMJjQLfKmLzD7xCDqD_BuzYEt-0"
RANGE_NAME = "AMZ US!A1:P"  # adjust later if ASIN/SKU are outside A:P


def main() -> None:
    load_dotenv()

    sa_path = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    service = build_sheets_service(sa_path)

    values = read_range(service, SPREADSHEET_ID, RANGE_NAME)

    # Drop leading empty rows (sometimes Sheets returns blank header padding)
    while values and (not values[0] or all(str(x).strip() == "" for x in values[0])):
        values = values[1:]

    df_raw = values_to_dataframe(values)
    df_clean = clean_gross_net_df(df_raw)

    mapping = select_gross_net_mapping(df_clean).copy()

    # Normalize column names defensively
    cols = {c.strip(): c for c in mapping.columns}
    # Expect these exact names after select_gross_net_mapping
    required = ["ASIN", "SKU"]
    missing = [c for c in required if c not in cols]
    if missing:
        raise RuntimeError(
            f"Gross&Net mapping missing required columns {missing}. "
            f"Got columns: {list(mapping.columns)}"
        )

    # Keep only what we need and clean values
    mapping = mapping[["ASIN", "SKU"]].copy()
    mapping["ASIN"] = mapping["ASIN"].astype(str).str.strip()
    mapping["SKU"] = mapping["SKU"].astype(str).str.strip()

    # Drop blanks
    mapping = mapping[(mapping["ASIN"] != "") & (mapping["SKU"] != "")]

    # De-dupe: if same ASIN maps to multiple SKUs, keep first and report it
    dup_asin = mapping["ASIN"].duplicated(keep=False)
    if dup_asin.any():
        dups = mapping.loc[dup_asin].sort_values("ASIN")
        print("WARNING: duplicate ASIN rows detected (showing first 20):")
        print(dups.head(20).to_string(index=False))

    mapping = mapping.drop_duplicates(subset=["ASIN"], keep="first").reset_index(drop=True)

    print("Gross&Net clean rows:", df_clean.shape)
    print("ASIN->SKU mapping rows:", mapping.shape)
    print("Sample mapping:")
    print(mapping.head(15).to_string(index=False))


if __name__ == "__main__":
    main()
