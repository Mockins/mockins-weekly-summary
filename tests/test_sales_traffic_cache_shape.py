from __future__ import annotations

import json
import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pytest

from weekly_summary.cache.sqlite_cache import CacheKey, get_cache_status, get_cached_parsed


REPORT_TYPE = "GET_SALES_AND_TRAFFIC_REPORT"


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _get_any_cached_keys_for_report(db_path: Path) -> list[dict]:
    """
    Introspect the cache DB and return all keys for GET_SALES_AND_TRAFFIC_REPORT.
    This avoids guessing what report_options_json you used in prior runs.
    """
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
              report_type,
              marketplace_id,
              data_start_date,
              data_end_date,
              report_options_json,
              status,
              created_at_utc,
              pulled_at_utc,
              expires_at_utc,
              report_id,
              document_id,
              row_count
            FROM spapi_parsed_cache
            WHERE report_type = ?
            ORDER BY created_at_utc DESC
            """,
            (REPORT_TYPE,),
        ).fetchall()

    return [dict(r) for r in rows]


@pytest.mark.parametrize("days_back", [1])
def test_sales_traffic_cached_payload_shape(days_back: int) -> None:
    """
    Cache-first diagnostic.

    Goal:
      - Prove what schema we are actually caching for a 1-day window:
          A) ASIN rows: payload has salesAndTrafficByAsin (good for per-ASIN Units)
          B) Date totals: payload has salesAndTrafficByDate only (cannot do per-ASIN Units)
      - Also prove whether the cached "parsed_obj" rows exist and what they look like.

    This test does NOT make any live SP-API calls.
    """
    db_path = Path("data") / "cache" / "spapi_reports.sqlite"
    if not db_path.exists():
        pytest.skip(f"No cache DB at {db_path}. Run your pipeline once to populate cache.")

    # Determine yesterday relative to local system date.
    # If you want to hardcode: end_date = date(2026, 2, 25)
    end_date = date.today() - timedelta(days=days_back)
    start_date = end_date

    # Find all cached Sales & Traffic entries so we can locate one for this date window.
    keys = _get_any_cached_keys_for_report(db_path)
    if not keys:
        pytest.skip("No cached GET_SALES_AND_TRAFFIC_REPORT entries found in sqlite cache.")

    # Prefer exact match on the 1-day window, regardless of report_options_json.
    matches = [
        k
        for k in keys
        if k["data_start_date"] == start_date.isoformat()
        and k["data_end_date"] == end_date.isoformat()
    ]

    if not matches:
        # If there is no exact 1-day cached window, don't fail—show what's available.
        available = sorted({(k["data_start_date"], k["data_end_date"], k["report_options_json"], k["status"]) for k in keys})
        pytest.skip(f"No cached 1-day window found for {start_date}..{end_date}. Available: {available}")

    # Use the most recently created matching entry
    chosen = matches[0]

    key = CacheKey(
        report_type=chosen["report_type"],
        marketplace_id=chosen["marketplace_id"],
        data_start_date=chosen["data_start_date"],
        data_end_date=chosen["data_end_date"],
        report_options_json=chosen["report_options_json"],
    )

    status = get_cache_status(db_path, key=key)
    assert status is not None

    print("\n--- Cache status ---")
    print(status)

    cached = get_cached_parsed(db_path, key=key)
    if cached is None:
        pytest.fail(
            "Cache row exists but get_cached_parsed returned None. "
            "Likely expired or status != OK. See printed cache status above."
        )

    # Your code caches parsed_obj={"rows": [...]}
    rows = cached.get("rows")
    assert rows is not None, f"Cached parsed object did not include 'rows'. Keys={list(cached.keys())}"
    assert isinstance(rows, list), f"cached['rows'] should be a list, got {type(rows)}"

    print("\n--- Cached parsed rows summary ---")
    print("rows_count:", len(rows))
    print("first_row:", rows[0] if rows else None)

    # If the cached parsed rows are ASIN-based, they should have asin and Units
    if rows:
        first = rows[0]
        # We don't assert strict schema (because you may have different parsers),
        # but we print diagnostics that tell us what it's doing.
        print("row_keys:", sorted(first.keys()))

    # The key test: detect whether cached rows are all zeros (symptom)
    if rows and isinstance(rows[0], dict) and "Units" in rows[0]:
        units = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            u = r.get("Units", 0)
            try:
                units.append(float(u))
            except Exception:
                pass

        if units:
            print("Units sum:", sum(units))
            print("Units max:", max(units))

    # This test should not "pass silently" if rows are empty—empty rows means you cannot compute per-ASIN units
    # from what you're caching for this window.
    assert len(rows) > 0, (
        "Cached parsed rows are empty for this 1-day window. "
        "That means the cache does not contain per-ASIN Units rows, so downstream windows will be zero."
    )