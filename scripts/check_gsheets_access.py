import os
from dotenv import load_dotenv

from weekly_summary.extract.google_sheets import build_sheets_service, read_range

# 1) Load .env
load_dotenv()

# 2) Load credentials path
sa_path = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]

# 3) Pick ONE sheet ID to test (Gross & Net is fine)
# Paste only the spreadsheet ID part (between /d/ and /edit) — NOT the full URL.
SPREADSHEET_ID = "1lJvclI4hgSYRpBTmVMJjQLfKmLzD7xCDqD_BuzYEt-0"


# 4) Read ONE cell from a tab you know exists
RANGE = "AMZ US!A1:Z5"


def main():
    service = build_sheets_service(sa_path)
    values = read_range(service, SPREADSHEET_ID, RANGE)
    print("Result:", values)


if __name__ == "__main__":
    main()
