from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

import pytest
from sp_api.api import Reports
from sp_api.base import Marketplaces

from weekly_summary.extract.amazon.report_utils import download_report_document, wait_for_report
from weekly_summary.extract.amazon.sales_traffic_by_window import _build_reports_client, _create_report_with_backoff

REPORT_TYPE = "GET_SALES_AND_TRAFFIC_REPORT"


@dataclass(frozen=True)
class ProbeResult:
    start_date: date
    end_date: date
    asin_granularity: str
    date_granularity: str
    report_id: str
    document_id: str
    has_by_asin_key: bool
    by_asin_len: Optional[int]
    first_row_keys: list[str]
    first_row_preview: str
    has_sku_field: bool
    has_child_asin_field: bool


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_preview(obj: Any, max_chars: int = 1200) -> str:
    try:
        return json.dumps(obj, indent=2, sort_keys=True)[:max_chars]
    except Exception:
        return str(obj)[:max_chars]


def _pull_payload(
    reports: Reports,
    *,
    marketplace_id: str,
    start_date: date,
    end_date: date,
    report_options: dict[str, str],
) -> tuple[str, str, dict]:
    start_dt = datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc)
    end_dt = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=timezone.utc)

    report_id = _create_report_with_backoff(
        reports,
        marketplace_ids=[marketplace_id],
        data_start_time=start_dt,
        data_end_time=end_dt,
        report_options=report_options,
    )
    document_id = wait_for_report(reports, report_id)

    doc = reports.get_report_document(reportDocumentId=document_id).payload
    raw = download_report_document(doc)
    payload = json.loads(raw.decode("utf-8", errors="replace").strip())
    assert isinstance(payload, dict)
    return report_id, document_id, payload


def _probe(
    *,
    reports: Reports,
    marketplace_id: str,
    start_date: date,
    end_date: date,
    asin_granularity: str,
    date_granularity: str = "DAY",
) -> ProbeResult:
    report_options = {"dateGranularity": date_granularity, "asinGranularity": asin_granularity}

    report_id, document_id, payload = _pull_payload(
        reports,
        marketplace_id=marketplace_id,
        start_date=start_date,
        end_date=end_date,
        report_options=report_options,
    )

    by_asin = payload.get("salesAndTrafficByAsin")
    has_by_asin_key = "salesAndTrafficByAsin" in payload
    by_asin_len: Optional[int] = None
    first_row_keys: list[str] = []
    first_row_preview = ""
    has_sku_field = False
    has_child_asin_field = False

    if isinstance(by_asin, list):
        by_asin_len = len(by_asin)
        if by_asin:
            first = by_asin[0]
            if isinstance(first, dict):
                first_row_keys = sorted(list(first.keys()))
                has_sku_field = ("sku" in first) or ("SKU" in first)
                has_child_asin_field = ("childAsin" in first) or ("(Child) ASIN" in first) or ("asin" in first)
                first_row_preview = _safe_preview(first)
            else:
                first_row_preview = _safe_preview(first)

    return ProbeResult(
        start_date=start_date,
        end_date=end_date,
        asin_granularity=asin_granularity,
        date_granularity=date_granularity,
        report_id=report_id,
        document_id=document_id,
        has_by_asin_key=has_by_asin_key,
        by_asin_len=by_asin_len,
        first_row_keys=first_row_keys,
        first_row_preview=first_row_preview,
        has_sku_field=has_sku_field,
        has_child_asin_field=has_child_asin_field,
    )


def _print_result(r: ProbeResult) -> None:
    print("\n=== PROBE RESULT ===")
    print("window:", r.start_date, "->", r.end_date)
    print("options:", {"asinGranularity": r.asin_granularity, "dateGranularity": r.date_granularity})
    print("report_id:", r.report_id)
    print("document_id:", r.document_id)
    print("has_salesAndTrafficByAsin_key:", r.has_by_asin_key)
    print("salesAndTrafficByAsin_len:", r.by_asin_len)
    print("first_row_keys:", r.first_row_keys)
    print("has_sku_field:", r.has_sku_field)
    print("has_child_asin_field:", r.has_child_asin_field)
    if r.first_row_preview:
        print("--- first_row_preview ---")
        print(r.first_row_preview)


@pytest.mark.live_spapi
@pytest.mark.parametrize("days_ago", [1, 2, 3, 7])
def test_probe_sku_granularity_by_asin_rows_availability(days_ago: int) -> None:
    """
    Determines whether asinGranularity=SKU actually yields salesAndTrafficByAsin rows,
    and whether those rows include SKU+childAsin.

    This test DOES make live SP-API calls and may take a while / throttle.
    """
    reports = _build_reports_client()
    end_date = date.today() - timedelta(days=days_ago)
    start_date = end_date

    r = _probe(
        reports=reports,
        marketplace_id=Marketplaces.US.marketplace_id,
        start_date=start_date,
        end_date=end_date,
        asin_granularity="SKU",
        date_granularity="DAY",
    )
    _print_result(r)

    # This is the key requirement for your LOC logic:
    # We need non-empty rows AND a sku field AND childAsin field.
    if r.by_asin_len and r.by_asin_len > 0:
        assert r.has_sku_field, "Got byAsin rows but no sku field; cannot detect -LOC."
        assert r.has_child_asin_field, "Got byAsin rows but no childAsin; cannot map reliably."
    else:
        # Don't fail yet; we're diagnosing which days are available.
        pytest.xfail("asinGranularity=SKU returned empty byAsin rows for this day (possible latency/unsupported).")


@pytest.mark.live_spapi
@pytest.mark.parametrize("days_ago", [1, 2, 3, 7])
def test_probe_asin_granularity_asin_baseline(days_ago: int) -> None:
    """
    Baseline: asinGranularity=ASIN should (usually) return salesAndTrafficByAsin rows.
    This confirms the report works at all for a given day.
    """
    reports = _build_reports_client()
    end_date = date.today() - timedelta(days=days_ago)
    start_date = end_date

    r = _probe(
        reports=reports,
        marketplace_id=Marketplaces.US.marketplace_id,
        start_date=start_date,
        end_date=end_date,
        asin_granularity="ASIN",
        date_granularity="DAY",
    )
    _print_result(r)

    assert r.by_asin_len is not None, "Expected salesAndTrafficByAsin key to exist."
    assert r.by_asin_len > 0, "asinGranularity=ASIN returned empty byAsin rows; report may be unavailable/latency."