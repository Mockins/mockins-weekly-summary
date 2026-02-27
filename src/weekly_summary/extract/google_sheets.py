from __future__ import annotations

import time
from typing import List

import httplib2
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_httplib2 import AuthorizedHttp

# Read-only scope (safest)
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def build_sheets_service(service_account_json_path: str, *, timeout_s: int = 120):
    """
    Build and return a Google Sheets API service client using
    a service account JSON key file.

    Uses explicit timeout + authorized HTTP transport.
    """
    credentials = Credentials.from_service_account_file(
        service_account_json_path,
        scopes=SCOPES,
    )

    base_http = httplib2.Http(timeout=timeout_s)
    authed_http = AuthorizedHttp(credentials, http=base_http)

    return build(
        "sheets",
        "v4",
        http=authed_http,
        cache_discovery=False,
    )


def read_range(
    service,
    spreadsheet_id: str,
    range_name: str,
    *,
    max_attempts: int = 3,
) -> List[List[str]]:
    """
    Read a range from a Google Sheet and return raw cell values.
    Retries transient errors.
    """
    last_err: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
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

        except (TimeoutError, HttpError) as e:
            last_err = e
            if attempt < max_attempts:
                time.sleep(3 * attempt)
            else:
                raise

    raise TimeoutError(f"Google Sheets read_range failed after {max_attempts} attempts: {last_err}")