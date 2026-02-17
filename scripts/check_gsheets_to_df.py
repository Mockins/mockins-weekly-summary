import os

from dotenv import load_dotenv

from weekly_summary.extract.google_sheets import build_sheets_service, read_range
from weekly_summary.transform.gsheets_to_df import values_to_dataframe
from weekly_summary.transform.gross_net_clean import clean_gross_net_df



def main() -> None:
    load_dotenv()

    sa_path = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]

    # Gross & Net sheet (already verified). This is the spreadsheet ID only.
    spreadsheet_id = "1lJvclI4hgSYRpBTmVMJjQLfKmLzD7xCDqD_BuzYEt-0"

    # Read a small slice so output is readable.
    # Adjust later once we finalize exact columns needed.
    range_name = "AMZ US!A1:P"

    service = build_sheets_service(sa_path)
    values = read_range(service, spreadsheet_id, range_name)

    print("First 3 rows raw:", values[:3])

    while values and (not values[0] or all(str(x).strip() == "" for x in values[0])):
        values = values[1:]

    df_raw = values_to_dataframe(values)
    df = clean_gross_net_df(df_raw)
    print(df.dtypes)


    print("Shape:", df.shape)
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
