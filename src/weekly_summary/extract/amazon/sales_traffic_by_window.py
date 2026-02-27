from __future__ import annotations

import json
import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from dotenv import load_dotenv
from sp_api.api import Reports
from sp_api.base import Marketplaces
from sp_api.base.exceptions import SellingApiForbiddenException, SellingApiRequestThrottledException

from weekly_summary.cache.sqlite_cache import (
    CacheKey,
    get_cache_status,
    get_cached_parsed,
    put_cache_error,
    put_cached_parsed,
)
from weekly_summary.extract.amazon.report_utils import download_report_document, wait_for_report

REPORT_TYPE = "GET_SALES_AND_TRAFFIC_REPORT"


class SalesTrafficSchemaError(ValueError):
    pass


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _build_reports_client() -> Reports:
    load_dotenv(override=True)

    refresh_token = os.getenv("SPAPI_REFRESH_TOKEN")
    lwa_app_id = os.getenv("SPAPI_LWA_APP_ID")
    lwa_client_secret = os.getenv("SPAPI_LWA_CLIENT_SECRET")

    if not refresh_token or not lwa_app_id or not lwa_client_secret:
        raise RuntimeError(
            "Missing SP-API env vars. Need SPAPI_REFRESH_TOKEN, SPAPI_LWA_APP_ID, SPAPI_LWA_CLIENT_SECRET"
        )

    return Reports(
        credentials={
            "refresh_token": refresh_token,
            "lwa_app_id": lwa_app_id,
            "lwa_client_secret": lwa_client_secret,
        },
        marketplace=Marketplaces.US,
    )


def _create_report_with_backoff(
    reports: Reports,
    *,
    marketplace_ids: list[str],
    data_start_time: datetime,
    data_end_time: datetime,
    report_options: dict[str, str],
    max_attempts: int = 8,
) -> str:
    for attempt in range(1, max_attempts + 1):
        try:
            res = reports.create_report(
                reportType=REPORT_TYPE,
                marketplaceIds=marketplace_ids,
                dataStartTime=_iso_utc(data_start_time),
                dataEndTime=_iso_utc(data_end_time),
                reportOptions=report_options,
            )
            report_id = (res.payload or {}).get("reportId")
            if not report_id:
                raise RuntimeError(f"No reportId in create_report payload: {res.payload}")
            return report_id

        except SellingApiRequestThrottledException:
            wait_s = min(30 * attempt, 180)
            print(f"Throttled on create_report. Waiting {wait_s}s (attempt {attempt}/{max_attempts})...")
            time.sleep(wait_s)

        except SellingApiForbiddenException as e:
            raise RuntimeError(f"Forbidden creating Sales & Traffic report. options={report_options}. err={e}") from e

    raise RuntimeError("Exceeded max attempts creating Sales & Traffic report due to throttling.")


def _ttl_seconds_for_window(*, end_date: date) -> int:
    """
    TTL policy:
      - If window ends yesterday or today: refresh more often (6 hours)
      - Otherwise: 30 days
    """
    today = date.today()
    if end_date >= (today - timedelta(days=1)):
        return 6 * 60 * 60
    return 30 * 24 * 60 * 60


def _pick_units_ordered_from_row(row: dict[str, Any]) -> float:
    """
    Observed payload for asinGranularity=SKU uses salesByAsin.unitsOrdered.
    Keep defensive fallback.
    """
    for container_key in ("salesBySku", "salesByAsin", "salesByDate"):
        container = row.get(container_key)
        if isinstance(container, dict) and container.get("unitsOrdered") is not None:
            try:
                return float(container.get("unitsOrdered"))
            except Exception:
                pass

    if row.get("unitsOrdered") is not None:
        try:
            return float(row.get("unitsOrdered"))
        except Exception:
            pass

    return 0.0


def _parse_rows_by_child_asin_and_sku(payload: Any) -> pd.DataFrame:
    if not isinstance(payload, dict):
        raise SalesTrafficSchemaError(f"Unexpected payload type: {type(payload)}")

    rows = payload.get("salesAndTrafficByAsin")

    # Schema must contain list; otherwise it's truly unexpected
    if not isinstance(rows, list):
        keys = list(payload.keys())[:60]
        raise SalesTrafficSchemaError(
            "Sales & Traffic payload missing salesAndTrafficByAsin or wrong type. "
            f"Top-level keys: {keys}"
        )

    # IMPORTANT: Amazon can return empty rows for most-recent day due to latency.
    # Treat as 0 sales and let caller fill 0s.
    if not rows:
        return pd.DataFrame(columns=["child_asin", "amazon_sku", "Units"])

    out: list[dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue

        child_asin = r.get("childAsin")
        amazon_sku = r.get("sku")
        if not child_asin or not amazon_sku:
            continue

        out.append(
            {
                "child_asin": str(child_asin).strip(),
                "amazon_sku": str(amazon_sku).strip(),
                "Units": _pick_units_ordered_from_row(r),
            }
        )

    df = pd.DataFrame(out)
    if df.empty:
        return pd.DataFrame(columns=["child_asin", "amazon_sku", "Units"])

    df["Units"] = pd.to_numeric(df["Units"], errors="coerce").fillna(0.0)
    df["child_asin"] = df["child_asin"].astype(str).str.strip()
    df["amazon_sku"] = df["amazon_sku"].astype(str).str.strip()

    return df.groupby(["child_asin", "amazon_sku"], as_index=False)["Units"].sum()


def get_sales_traffic_rows_cached(
    *,
    start_date: date,
    end_date: date,
    marketplace_id: str = Marketplaces.US.marketplace_id,
    asin_granularity: str = "SKU",  # required for sku column and LOC detection
    date_granularity: str = "DAY",
    db_path: Path = Path("data") / "cache" / "spapi_reports.sqlite",
    reuse_cache: bool = True,
    debug_cache_status: bool = False,
) -> pd.DataFrame:
    """
    Pull one Sales & Traffic report for the requested window and return row-level data:
      child_asin, amazon_sku, Units
    """
    report_options = {"dateGranularity": date_granularity, "asinGranularity": asin_granularity}
    report_options_json = json.dumps(report_options, separators=(",", ":"), sort_keys=True)

    key = CacheKey(
        report_type=REPORT_TYPE,
        marketplace_id=marketplace_id,
        data_start_date=start_date.isoformat(),
        data_end_date=end_date.isoformat(),
        report_options_json=report_options_json,
    )

    if debug_cache_status:
        st = get_cache_status(db_path, key=key)
        if st is not None:
            print("Cache status:", st)

    if reuse_cache:
        cached = get_cached_parsed(db_path, key=key)
        if cached is not None:
            rows = cached.get("rows", [])
            df = pd.DataFrame(rows)
            if df.empty:
                return pd.DataFrame(columns=["child_asin", "amazon_sku", "Units"])
            df["Units"] = pd.to_numeric(df["Units"], errors="coerce").fillna(0.0)
            df["child_asin"] = df["child_asin"].astype(str).str.strip()
            df["amazon_sku"] = df["amazon_sku"].astype(str).str.strip()
            return df[["child_asin", "amazon_sku", "Units"]]

    reports = _build_reports_client()

    start_dt = datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc)
    end_dt = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=timezone.utc)

    print(f"Sales&Traffic window pull: {start_date} -> {end_date} options={report_options}")

    report_id: Optional[str] = None
    document_id: Optional[str] = None
    raw: Optional[bytes] = None
    pulled_at_utc = _utc_now_iso()
    ttl_seconds = _ttl_seconds_for_window(end_date=end_date)

    try:
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

        try:
            payload = json.loads(raw.decode("utf-8", errors="replace").strip())
        except Exception as e:
            preview = raw[:800].decode("utf-8", errors="replace")
            raise SalesTrafficSchemaError(f"Downloaded document was not valid JSON. Preview: {preview}") from e

        df_rows = _parse_rows_by_child_asin_and_sku(payload)

        put_cached_parsed(
            db_path,
            key=key,
            parsed_obj={"rows": df_rows.to_dict(orient="records")},
            ttl_seconds=ttl_seconds,
            pulled_at_utc=pulled_at_utc,
            report_id=report_id,
            document_id=document_id,
            raw_bytes=raw,
            row_count=int(len(df_rows)),
        )

        return df_rows[["child_asin", "amazon_sku", "Units"]]

    except Exception as e:
        put_cache_error(
            db_path,
            key=key,
            error_message=f"{type(e).__name__}: {e}",
            ttl_seconds=15 * 60,
            pulled_at_utc=pulled_at_utc,
            report_id=report_id,
            document_id=document_id,
        )
        raise


def get_units_by_asin_cached(**kwargs: Any) -> pd.DataFrame:
    raise RuntimeError(
        "get_units_by_asin_cached is deprecated for this workflow. "
        "Use get_sales_traffic_rows_cached() so we can detect -LOC via amazon_sku."
    )