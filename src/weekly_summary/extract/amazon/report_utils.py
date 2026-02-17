from __future__ import annotations

import gzip
import io
import json
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from sp_api.api import Reports


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(dt: datetime) -> str:
    return dt.isoformat()


def download_report_document(doc: dict, timeout_seconds: int = 120) -> bytes:
    """
    Download the report document from the pre-signed URL.
    Decompress if compressionAlgorithm is GZIP.
    """
    url = doc["url"]
    resp = requests.get(url, timeout=timeout_seconds)
    resp.raise_for_status()
    raw = resp.content

    if doc.get("compressionAlgorithm") == "GZIP":
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as gz:
            return gz.read()

    return raw


def wait_for_report(
    reports: Reports,
    report_id: str,
    poll_interval_seconds: int = 10,
    max_wait_seconds: int = 3600,
) -> str:
    """
    Poll until report is DONE and return reportDocumentId.
    """
    elapsed = 0
    while elapsed < max_wait_seconds:
        payload = reports.get_report(reportId=report_id).payload
        status = payload.get("processingStatus")
        print(f"  status={status}")

        if status == "DONE":
            return payload["reportDocumentId"]

        if status in ("CANCELLED", "FATAL"):
            raise RuntimeError(f"Report failed: {status}. Payload: {json.dumps(payload)}")

        time.sleep(poll_interval_seconds)
        elapsed += poll_interval_seconds

    raise TimeoutError(f"Report {report_id} did not complete within {max_wait_seconds}s")
