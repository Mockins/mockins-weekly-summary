from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional


@dataclass(frozen=True)
class CacheKey:
    report_type: str
    marketplace_id: str
    data_start_date: str  # YYYY-MM-DD
    data_end_date: str    # YYYY-MM-DD
    report_options_json: str  # stable JSON string (sorted keys)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _utc_now_iso() -> str:
    return _utc_now().isoformat()


def _iso_to_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _ensure_parent_dir(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)


def _connect(db_path: Path) -> sqlite3.Connection:
    _ensure_parent_dir(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: Path) -> None:
    """
    Create table if missing AND migrate older schemas by adding missing columns.
    """
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS spapi_parsed_cache (
              report_type TEXT NOT NULL,
              marketplace_id TEXT NOT NULL,
              data_start_date TEXT NOT NULL,
              data_end_date TEXT NOT NULL,
              report_options_json TEXT NOT NULL,

              status TEXT NOT NULL,                -- OK | ERROR
              parsed_json TEXT,                    -- only when status=OK
              error_message TEXT,                  -- only when status=ERROR

              created_at_utc TEXT NOT NULL,
              pulled_at_utc TEXT,                  -- when data was pulled from SP-API
              expires_at_utc TEXT,                 -- when this cache entry becomes invalid

              report_id TEXT,
              document_id TEXT,
              payload_sha256 TEXT,                 -- hash of raw downloaded bytes (post decrypt/decompress)
              row_count INTEGER,

              PRIMARY KEY (report_type, marketplace_id, data_start_date, data_end_date, report_options_json)
            )
            """
        )

        cols = {r["name"] for r in conn.execute("PRAGMA table_info(spapi_parsed_cache)").fetchall()}

        def add_col(name: str, col_def: str) -> None:
            if name not in cols:
                conn.execute(f"ALTER TABLE spapi_parsed_cache ADD COLUMN {name} {col_def}")

        # Migrations for old DB files (add new columns as needed)
        add_col("status", "TEXT")
        add_col("parsed_json", "TEXT")
        add_col("error_message", "TEXT")
        add_col("created_at_utc", "TEXT")
        add_col("pulled_at_utc", "TEXT")
        add_col("expires_at_utc", "TEXT")
        add_col("report_id", "TEXT")
        add_col("document_id", "TEXT")
        add_col("payload_sha256", "TEXT")
        add_col("row_count", "INTEGER")

        # Backfill minimal defaults for old rows if any
        conn.execute("UPDATE spapi_parsed_cache SET status = COALESCE(status, 'OK')")

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_spapi_parsed_cache_created_at
            ON spapi_parsed_cache(created_at_utc)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_spapi_parsed_cache_expires_at
            ON spapi_parsed_cache(expires_at_utc)
            """
        )
        conn.commit()


def _is_expired(expires_at_utc: Optional[str]) -> bool:
    if not expires_at_utc:
        return False
    try:
        return _utc_now() >= _iso_to_dt(expires_at_utc)
    except Exception:
        return True


def get_cache_status(db_path: Path, *, key: CacheKey) -> Optional[dict[str, Any]]:
    init_db(db_path)
    with _connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT
              status,
              error_message,
              created_at_utc,
              pulled_at_utc,
              expires_at_utc,
              report_id,
              document_id,
              payload_sha256,
              row_count
            FROM spapi_parsed_cache
            WHERE report_type = ?
              AND marketplace_id = ?
              AND data_start_date = ?
              AND data_end_date = ?
              AND report_options_json = ?
            """,
            (
                key.report_type,
                key.marketplace_id,
                key.data_start_date,
                key.data_end_date,
                key.report_options_json,
            ),
        ).fetchone()

        if not row:
            return None

        return {
            "status": row["status"],
            "error_message": row["error_message"],
            "created_at_utc": row["created_at_utc"],
            "pulled_at_utc": row["pulled_at_utc"],
            "expires_at_utc": row["expires_at_utc"],
            "is_expired": _is_expired(row["expires_at_utc"]),
            "report_id": row["report_id"],
            "document_id": row["document_id"],
            "payload_sha256": row["payload_sha256"],
            "row_count": row["row_count"],
        }


def get_cached_parsed(db_path: Path, *, key: CacheKey) -> Optional[dict[str, Any]]:
    init_db(db_path)
    with _connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT status, parsed_json, expires_at_utc
            FROM spapi_parsed_cache
            WHERE report_type = ?
              AND marketplace_id = ?
              AND data_start_date = ?
              AND data_end_date = ?
              AND report_options_json = ?
            """,
            (
                key.report_type,
                key.marketplace_id,
                key.data_start_date,
                key.data_end_date,
                key.report_options_json,
            ),
        ).fetchone()

        if not row:
            return None
        if row["status"] != "OK":
            return None
        if _is_expired(row["expires_at_utc"]):
            return None

        parsed_json = row["parsed_json"]
        if not parsed_json:
            return None

        return json.loads(parsed_json)


def delete_expired_rows(db_path: Path) -> int:
    init_db(db_path)
    now_iso = _utc_now_iso()
    with _connect(db_path) as conn:
        cur = conn.execute(
            """
            DELETE FROM spapi_parsed_cache
            WHERE expires_at_utc IS NOT NULL
              AND expires_at_utc <= ?
            """,
            (now_iso,),
        )
        conn.commit()
        return int(cur.rowcount or 0)


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def put_cached_parsed(
    db_path: Path,
    *,
    key: CacheKey,
    parsed_obj: dict[str, Any],
    ttl_seconds: Optional[int] = None,
    pulled_at_utc: Optional[str] = None,
    report_id: Optional[str] = None,
    document_id: Optional[str] = None,
    raw_bytes: Optional[bytes] = None,
    row_count: Optional[int] = None,
) -> None:
    init_db(db_path)

    created_at = _utc_now()
    expires_at = None
    if ttl_seconds is not None:
        expires_at = (created_at + timedelta(seconds=int(ttl_seconds))).isoformat()

    payload_sha = _sha256_hex(raw_bytes) if raw_bytes is not None else None

    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO spapi_parsed_cache (
              report_type, marketplace_id, data_start_date, data_end_date, report_options_json,
              status, parsed_json, error_message,
              created_at_utc, pulled_at_utc, expires_at_utc,
              report_id, document_id, payload_sha256, row_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                key.report_type,
                key.marketplace_id,
                key.data_start_date,
                key.data_end_date,
                key.report_options_json,
                "OK",
                json.dumps(parsed_obj, separators=(",", ":"), sort_keys=True),
                None,
                created_at.isoformat(),
                pulled_at_utc,
                expires_at,
                report_id,
                document_id,
                payload_sha,
                row_count,
            ),
        )
        conn.commit()


def put_cache_error(
    db_path: Path,
    *,
    key: CacheKey,
    error_message: str,
    ttl_seconds: Optional[int] = 600,
    pulled_at_utc: Optional[str] = None,
    report_id: Optional[str] = None,
    document_id: Optional[str] = None,
) -> None:
    init_db(db_path)

    created_at = _utc_now()
    expires_at = None
    if ttl_seconds is not None:
        expires_at = (created_at + timedelta(seconds=int(ttl_seconds))).isoformat()

    # parsed_json is NOT NULL in schema, so store a minimal error payload
    payload = {
        "error": (error_message or "")[:2000],
        "report_type": key.report_type,
        "marketplace_id": key.marketplace_id,
        "data_start_date": key.data_start_date,
        "data_end_date": key.data_end_date,
    }
    parsed_json = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    payload_sha256 = hashlib.sha256(parsed_json.encode("utf-8")).hexdigest()

    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO spapi_parsed_cache (
              report_type, marketplace_id, data_start_date, data_end_date, report_options_json,
              status, parsed_json, error_message,
              created_at_utc, pulled_at_utc, expires_at_utc,
              report_id, document_id, payload_sha256, row_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                key.report_type,
                key.marketplace_id,
                key.data_start_date,
                key.data_end_date,
                key.report_options_json,
                "ERROR",
                parsed_json,
                (error_message or "")[:2000],
                created_at.isoformat(),
                pulled_at_utc,
                expires_at,
                report_id,
                document_id,
                payload_sha256,
                0,
            ),
        )
        conn.commit()