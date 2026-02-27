from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest
from sp_api.base import Marketplaces

from weekly_summary.cache.sqlite_cache import CacheKey, get_cache_status, get_cached_parsed

REPORT_TYPE = "GET_SALES_AND_TRAFFIC_REPORT"


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _latest_key_for_window(db_path: Path, start_date: date, end_date: date) -> CacheKey:
    with _connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT report_type, marketplace_id, data_start_date, data_end_date, report_options_json
            FROM spapi_parsed_cache
            WHERE report_type = ?
              AND data_start_date = ?
              AND data_end_date = ?
            ORDER BY created_at_utc DESC
            LIMIT 1
            """,
            (REPORT_TYPE, start_date.isoformat(), end_date.isoformat()),
        ).fetchone()
    if not row:
        raise AssertionError(f"No cached row for {start_date}..{end_date}")
    return CacheKey(
        report_type=row["report_type"],
        marketplace_id=row["marketplace_id"],
        data_start_date=row["data_start_date"],
        data_end_date=row["data_end_date"],
        report_options_json=row["report_options_json"],
    )


def test_cached_units_for_specific_asin_1day() -> None:
    db_path = Path("data") / "cache" / "spapi_reports.sqlite"
    if not db_path.exists():
        pytest.skip(f"No cache DB at {db_path}")

    end_date = date.today() - timedelta(days=1)
    start_date = end_date

    key = _latest_key_for_window(db_path, start_date, end_date)
    assert key.marketplace_id == Marketplaces.US.marketplace_id

    st = get_cache_status(db_path, key=key)
    assert st is not None
    print("\n--- Cache key used ---")
    print(key)
    print("\n--- Cache status ---")
    print(st)

    cached = get_cached_parsed(db_path, key=key)
    assert cached is not None
    df = pd.DataFrame(cached.get("rows", []))
    assert not df.empty, "cached rows empty"

    df["asin"] = df["asin"].astype(str).str.strip()
    df["Units"] = pd.to_numeric(df["Units"], errors="coerce").fillna(0.0)

    target_asin = "B082463V8J"
    sub = df[df["asin"] == target_asin]
    units = float(sub["Units"].sum()) if not sub.empty else 0.0

    print("\n--- Target ASIN ---")
    print("asin:", target_asin)
    print("units_in_cached_rows:", units)
    print("distinct_asins_in_report:", df["asin"].nunique())
    print("total_units_sum_report:", float(df["Units"].sum()))

    # No assert yet; we’re diagnosing.