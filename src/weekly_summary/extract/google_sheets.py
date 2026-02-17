from __future__ import annotations

from typing import List

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Read-only scope (safest)
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def build_sheets_service(service_account_json_path: str):
    """
    Build and return a Google Sheets API service client using
    a service account JSON key file.

    This function does NOT make any API calls by itself.
    """
    credentials = Credentials.from_service_account_file(
        service_account_json_path,
        scopes=SCOPES,
    )
    return build("sheets", "v4", credentials=credentials)


def read_range(
    service,
    spreadsheet_id: str,
    range_name: str,
) -> List[List[str]]:
    """
    Read a range from a Google Sheet and return the raw cell values.

    Example range_name: "AMZ US!A1:K500"
    """
    result = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=spreadsheet_id,
            range=range_name,
        )
        .execute()
    )
    return result.get("values", [])

