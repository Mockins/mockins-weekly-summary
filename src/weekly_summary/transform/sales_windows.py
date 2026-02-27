from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from sp_api.base import Marketplaces

from weekly_summary.extract.amazon.sales_traffic_by_window import get_sales_traffic_rows_cached


@dataclass(frozen=True)
class Window:
    name: str
    start: date
    end: date


def build_windows(*, end_date: date) -> list[Window]:
    return [
        Window("1 Day", end_date, end_date),
        Window("7 Days", end_date - timedelta(days=6), end_date),
        Window("8-14", end_date - timedelta(days=13), end_date - timedelta(days=7)),
        Window("15-21", end_date - timedelta(days=20), end_date - timedelta(days=14)),
        Window("22-28", end_date - timedelta(days=27), end_date - timedelta(days=21)),
        Window("1-28", end_date - timedelta(days=27), end_date),
        Window("29-56", end_date - timedelta(days=55), end_date - timedelta(days=28)),
        Window("57-84", end_date - timedelta(days=83), end_date - timedelta(days=56)),
    ]


def _normalize_mapping(asin_sku_map: pd.DataFrame) -> pd.DataFrame:
    """
    Gross & Net sheet mapping (ASIN -> base SKU).
    We strip any -loc suffixes in the mapping so we can apply them ourselves.
    """
    m = asin_sku_map.rename(columns={"ASIN": "child_asin", "SKU": "mapped_sku"}).copy()
    m["child_asin"] = (
        m["child_asin"].astype(str).str.strip().str.replace(r"(?i)-loc$", "", regex=True)
    )
    m["mapped_sku"] = (
        m["mapped_sku"].astype(str).str.strip().str.replace(r"(?i)-loc$", "", regex=True)
    )
    m = m[(m["child_asin"].str.len() > 0) & (m["mapped_sku"].str.len() > 0)]
    return m.drop_duplicates(subset=["child_asin"], keep="first")


def _apply_loc_output_keys(df: pd.DataFrame) -> pd.DataFrame:
    """
    Input columns: child_asin, amazon_sku, Units, mapped_sku
    Output columns:
      asin_out  - child_asin or child_asin+'-loc'
      sku_out   - mapped_sku or mapped_sku+'-LOC'
      Units
    """
    out = df.copy()
    out["child_asin"] = out["child_asin"].astype(str).str.strip()
    out["mapped_sku"] = out["mapped_sku"].astype(str).str.strip()
    out["amazon_sku"] = out["amazon_sku"].astype(str).str.strip()
    out["Units"] = pd.to_numeric(out["Units"], errors="coerce").fillna(0.0)

    loc_mask = out["amazon_sku"].str.upper().str.endswith("-LOC")

    out["asin"] = out["child_asin"]
    out["sku"] = out["mapped_sku"]

    out.loc[loc_mask, "asin"] = out.loc[loc_mask, "asin"] + "-loc"
    out.loc[loc_mask, "sku"] = out.loc[loc_mask, "sku"] + "-LOC"

    return out[["asin", "sku", "Units"]]


def compute_sku_sales_windows(
    *,
    end_date: date,
    asin_sku_map: pd.DataFrame,  # columns: ASIN, SKU (gross & net)
    db_path: Path = Path("data") / "cache" / "spapi_reports.sqlite",
    marketplace_id: str = Marketplaces.US.marketplace_id,
    reuse_cache: bool = True,
) -> pd.DataFrame:
    """
    Output is like the original (SKU-level window totals), but with MORE ROWS:
      - base sku rows (mapped from gross/net)
      - LOC sku rows: mapped_sku + '-LOC'
    and includes the corresponding asin (with -loc for LOC rows).

    IMPORTANT:
    - We DO NOT use Amazon's SKU for naming.
      Amazon SKU is used ONLY to detect whether the row is a -LOC variant.
    """
    windows = build_windows(end_date=end_date)
    mapping = _normalize_mapping(asin_sku_map)

    out: pd.DataFrame | None = None

    for win in windows:
        df_rows = get_sales_traffic_rows_cached(
            start_date=win.start,
            end_date=win.end,
            marketplace_id=marketplace_id,
            db_path=db_path,
            reuse_cache=reuse_cache,
        )

        if df_rows.empty:
            df_win = pd.DataFrame({"sku": [], "asin": [], win.name: []})
        else:
            df_rows["child_asin"] = df_rows["child_asin"].astype(str).str.strip()
            df_rows["amazon_sku"] = df_rows["amazon_sku"].astype(str).str.strip()
            df_rows["Units"] = pd.to_numeric(df_rows["Units"], errors="coerce").fillna(0.0)

            df = df_rows.merge(mapping, on="child_asin", how="left")
            df = df[df["mapped_sku"].notna() & (df["mapped_sku"].astype(str).str.len() > 0)].copy()

            df_prod = _apply_loc_output_keys(df)

            # Group by output sku+asin so LOC stays a distinct row
            df_win = (
                df_prod.groupby(["sku", "asin"], as_index=False)["Units"]
                .sum()
                .rename(columns={"Units": win.name})
            )

        out = df_win if out is None else out.merge(df_win, on=["sku", "asin"], how="outer")

    if out is None:
        out = pd.DataFrame({"sku": [], "asin": []})

    out = out.fillna(0)

    # Averages
    if all(c in out.columns for c in ["7 Days", "8-14", "15-21", "22-28"]):
        out["4 Week Avg"] = (out["7 Days"] + out["8-14"] + out["15-21"] + out["22-28"]) / 4.0
    else:
        out["4 Week Avg"] = 0.0

    if all(c in out.columns for c in ["1-28", "29-56", "57-84"]):
        out["3 Month Avg"] = (out["1-28"] + out["29-56"] + out["57-84"]) / 3.0
    else:
        out["3 Month Avg"] = 0.0

    # Formatting: integer windows, 1-decimal averages
    window_cols = ["1 Day", "7 Days", "8-14", "15-21", "22-28", "1-28", "29-56", "57-84"]
    for c in window_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).round(0).astype("int64")

    for c in ["4 Week Avg", "3 Month Avg"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).round(1)

    return out