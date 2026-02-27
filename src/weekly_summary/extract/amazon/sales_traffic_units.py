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
            wait_s = 30 * attempt
            print(f"Throttled on create_report. Waiting {wait_s}s (attempt {attempt}/{max_attempts})...")
            time.sleep(wait_s)

        except SellingApiForbiddenException as e:
            raise RuntimeError(f"Forbidden creating Sales & Traffic report. options={report_options}. err={e}") from e

    raise RuntimeError("Exceeded max attempts creating Sales & Traffic report due to throttling.")


def _iter_asin_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("salesAndTrafficByAsin")
    if isinstance(rows, list) and rows:
        return [x for x in rows if isinstance(x, dict)]

    by_date = payload.get("salesAndTrafficByDate")
    if isinstance(by_date, list) and by_date:
        out: list[dict[str, Any]] = []
        for day in by_date:
            if not isinstance(day, dict):
                continue
            r = day.get("salesAndTrafficByAsin")
            if isinstance(r, list) and r:
                out.extend([x for x in r if isinstance(x, dict)])
        if out:
            return out

    return []


def _parse_units_rows(payload: Any) -> pd.DataFrame:
    """
    Returns DataFrame with columns:
      parentAsin (optional), childAsin (optional), sku (optional), Units (required)
    """
    if not isinstance(payload, dict):
        raise SalesTrafficSchemaError(f"Unexpected payload type: {type(payload)}")

    rows = _iter_asin_rows(payload)
    if not rows:
        keys = list(payload.keys())[:40]
        raise SalesTrafficSchemaError(
            "Sales & Traffic payload did not contain any asin-level rows. "
            f"Top-level keys: {keys}"
        )

    df = pd.json_normalize(rows)

    parent_col = "parentAsin" if "parentAsin" in df.columns else None
    child_col = "childAsin" if "childAsin" in df.columns else None
    sku_col = "sku" if "sku" in df.columns else ("SKU" if "SKU" in df.columns else None)

    units_col = None
    for c in ("salesByAsin.unitsOrdered", "unitsOrdered"):
        if c in df.columns:
            units_col = c
            break
    if not units_col:
        raise SalesTrafficSchemaError(f"No unitsOrdered column found. Columns: {list(df.columns)[:120]}")

    out_data: dict[str, Any] = {
        "Units": pd.to_numeric(df[units_col], errors="coerce").fillna(0.0),
    }
    if parent_col:
        out_data["parentAsin"] = df[parent_col].astype(str).str.strip()
    if child_col:
        out_data["childAsin"] = df[child_col].astype(str).str.strip()
    if sku_col:
        out_data["sku"] = df[sku_col].astype(str).str.strip()

    out = pd.DataFrame(out_data)

    for c in ("parentAsin", "childAsin", "sku"):
        if c in out.columns:
            out[c] = out[c].replace({"None": pd.NA, "nan": pd.NA, "": pd.NA})

    group_cols = [c for c in ("parentAsin", "childAsin", "sku") if c in out.columns]
    if group_cols:
        out = out.groupby(group_cols, as_index=False)["Units"].sum()
    else:
        out = pd.DataFrame({"Units": [float(out["Units"].sum())]})

    return out


def _ttl_seconds_for_range(*, start_date: date, end_date: date) -> int:
    """
    TTL policy:
    - If range ends within the last 2 days (yesterday or today), TTL = 6 hours
    - Else TTL = 30 days
    """
    today = date.today()
    if end_date >= (today - timedelta(days=2)):
        return 6 * 60 * 60
    return 30 * 24 * 60 * 60


def get_units_rows_cached(
    *,
    start_date: date,
    end_date: date,
    marketplace_id: str = Marketplaces.US.marketplace_id,
    asin_granularity: str = "ASIN",
    date_granularity: str = "DAY",
    db_path: Path = Path("data") / "cache" / "spapi_reports.sqlite",
    reuse_cache: bool = True,
    debug_cache_status: bool = False,
) -> pd.DataFrame:
    report_options: dict[str, str] = {"dateGranularity": date_granularity, "asinGranularity": asin_granularity}
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

            # Make downstream stable: ensure expected columns exist
            for c in ("parentAsin", "childAsin", "sku"):
                if c not in df.columns:
                    df[c] = pd.NA

            if "Units" not in df.columns:
                df["Units"] = 0.0
            df["Units"] = pd.to_numeric(df["Units"], errors="coerce").fillna(0.0)

            return df

    reports = _build_reports_client()

    start_dt = datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc)
    end_dt = datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59, tzinfo=timezone.utc)

    print(f"Sales&Traffic (cache miss): {start_date}..{end_date} options={report_options}")

    report_id: Optional[str] = None
    document_id: Optional[str] = None
    raw: Optional[bytes] = None
    pulled_at_utc = _utc_now_iso()

    ttl_seconds = _ttl_seconds_for_range(start_date=start_date, end_date=end_date)

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
            preview = raw[:600].decode("utf-8", errors="replace")
            raise SalesTrafficSchemaError(f"Downloaded document was not valid JSON. Preview: {preview}") from e

        df_rows = _parse_units_rows(payload)

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

        return df_rows

    except Exception as e:
        put_cache_error(
            db_path,
            key=key,
            error_message=f"{type(e).__name__}: {e} | range={start_date}..{end_date} options={report_options}",
            ttl_seconds=15 * 60,
            pulled_at_utc=pulled_at_utc,
            report_id=report_id,
            document_id=document_id,
        )
        raise