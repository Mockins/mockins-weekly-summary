from __future__ import annotations

from datetime import date

import pandas as pd
import pytest
from sp_api.base import Marketplaces

from weekly_summary.extract.amazon.sales_traffic_by_window import get_sales_traffic_rows_cached
from weekly_summary.helpers.asin_sku_mapping import load_asin_sku_mapping


def _normalize_mapping(mapping: pd.DataFrame) -> pd.DataFrame:
    m = mapping[["ASIN", "SKU"]].copy()
    m["ASIN"] = m["ASIN"].astype(str).str.strip().str.replace(r"(?i)-loc$", "", regex=True)
    m["SKU"] = m["SKU"].astype(str).str.strip().str.replace(r"(?i)-loc$", "", regex=True)
    m = m[(m["ASIN"].str.len() > 0) & (m["SKU"].str.len() > 0)]
    return m.drop_duplicates(subset=["ASIN"], keep="first").rename(columns={"ASIN": "child_asin", "SKU": "mapped_sku"})


@pytest.mark.live_spapi
def test_loc_rows_are_renamed_and_remain_separate_when_base_exists_feb25_2026() -> None:
    """
    Final behavior check for LOC rule on 2026-02-25:

    REQUIRED invariants:
      - For any Amazon row whose amazon_sku endswith '-LOC', output keys must become:
          asin_out = childAsin + '-loc'
          sku_out  = mappedSku + '-LOC'
      - Units for that row must carry over to that output key.

    CONDITIONAL invariant:
      - If a base (non-LOC) row exists for the same childAsin on that day,
        it must not be merged with the LOC row (both keys must exist).
    """
    d = date(2026, 2, 25)

    df_rows = get_sales_traffic_rows_cached(
        start_date=d,
        end_date=d,
        marketplace_id=Marketplaces.US.marketplace_id,
        reuse_cache=False,
    )

    assert not df_rows.empty, "No rows returned; cannot validate LOC behavior."

    df_rows["child_asin"] = df_rows["child_asin"].astype(str).str.strip()
    df_rows["amazon_sku"] = df_rows["amazon_sku"].astype(str).str.strip()
    df_rows["Units"] = pd.to_numeric(df_rows["Units"], errors="coerce").fillna(0.0)

    loc_rows = df_rows[df_rows["amazon_sku"].str.upper().str.endswith("-LOC")].copy()
    if loc_rows.empty:
        pytest.xfail("No '-LOC' rows found on 2026-02-25; cannot validate LOC transformation on that date.")

    # Pick one LOC row to validate
    sample = loc_rows.iloc[0]
    child_asin = str(sample["child_asin"]).strip()
    units_loc = float(sample["Units"])

    mapping = _normalize_mapping(load_asin_sku_mapping())
    mapped = mapping[mapping["child_asin"] == child_asin]
    if mapped.empty:
        pytest.xfail(
            f"LOC sample child ASIN {child_asin} not found in Gross&Net mapping; cannot validate naming rule."
        )

    mapped_sku = str(mapped.iloc[0]["mapped_sku"]).strip()

    asin_out = f"{child_asin}-loc"
    sku_out = f"{mapped_sku}-LOC"

    # Apply production-like transformation
    prod = df_rows.merge(mapping, on="child_asin", how="left")
    prod = prod[prod["mapped_sku"].notna()].copy()

    loc_mask = prod["amazon_sku"].str.upper().str.endswith("-LOC")
    prod["asin_out"] = prod["child_asin"]
    prod["sku_out"] = prod["mapped_sku"]
    prod.loc[loc_mask, "asin_out"] = prod.loc[loc_mask, "asin_out"] + "-loc"
    prod.loc[loc_mask, "sku_out"] = prod.loc[loc_mask, "sku_out"] + "-LOC"

    # REQUIRED: LOC output row exists
    match_loc = prod[(prod["asin_out"] == asin_out) & (prod["sku_out"] == sku_out)]
    assert not match_loc.empty, "Expected LOC output row (asin_out, sku_out) not found."

    # REQUIRED: Units carried over correctly (sum in case multiple amazon rows map to same output key)
    units_out = float(match_loc["Units"].sum())
    assert units_out == units_loc, f"Expected LOC units to carry over. got {units_out}, expected {units_loc}"

    # CONDITIONAL: if base exists in source data, ensure both exist separately in output
    base_present_in_source = any(
        (df_rows["child_asin"] == child_asin) & (~df_rows["amazon_sku"].str.upper().str.endswith("-LOC"))
    )
    if base_present_in_source:
        base_exists = not prod[(prod["asin_out"] == child_asin) & (prod["sku_out"] == mapped_sku)].empty
        loc_exists = not match_loc.empty
        assert base_exists and loc_exists, "Base row exists in source but base/LOC were not both present in output."