import os
from dotenv import load_dotenv

from weekly_summary.extract.google_sheets import build_sheets_service, read_range
from weekly_summary.transform.gsheets_to_df import values_to_dataframe, slice_values_from_header



def main() -> None:
    load_dotenv()
    sa_path = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]

    # Weights & Dims spreadsheet
    spreadsheet_id = "1J4hIViDyNEIBmZsrR1yGoK-a70Mhh0kRtUcnJZ7qEck"

    # Master Carton tab:
    # B = SKU, I = Qty per carton
    # We'll pull a wider range so we can reliably select columns by name.
    range_name = "Master Carton!A1:Z60"


    service = build_sheets_service(sa_path)
    values = read_range(service, spreadsheet_id, range_name)

    values = slice_values_from_header(values, "Mini SKU:")

    print("Header row: ", values[0])

    # print("RAW first 30 rows:")
    # for i, r in enumerate(values[:30], start=1):
    #     print(f"{i:02d}: {r}")


    df = values_to_dataframe(values)

    print("Master Carton rows/cols:", df.shape)
    print("Columns:", list(df.columns))
    print(df[["Mini SKU:", "Qty per Master:"]].head(10).to_string(index=False))


if __name__ == "__main__":
    main()
