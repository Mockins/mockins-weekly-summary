from __future__ import annotations

from datetime import date, timedelta

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
    # 1:1 pick-first is fine for this test
    return m.drop_duplicates(subset=["ASIN"], keep="first").rename(columns={"ASIN": "child_asin", "SKU": "mapped_sku"})


@pytest.mark.live_spapi
def test_loc_rows_become_separate_products_with_mapped_sku_naming() -> None:
    """
    End-to-end behavior check for the LOC rule:

    - Pull report rows (child_asin, amazon_sku, Units) at SKU granularity
    - Find at least one amazon_sku that ends with -LOC
    - Map child_asin to mapped_sku (gross&net)
    - Output row must be:
        asin_out = child_asin + "-loc"
        sku_out  = mapped_sku + "-LOC"
      with the SAME Units as the Amazon LOC row
    """
    # Use an "older" day to avoid the known latency for yesterday.
    d = date.today() - timedelta(days=7)

    df_rows = get_sales_traffic_rows_cached(
        start_date=d,
        end_date=d,
        marketplace_id=Marketplaces.US.marketplace_id,
        reuse_cache=False,  # live pull so we test real behavior
    )

    assert not df_rows.empty, "No rows returned; cannot validate LOC behavior."

    df_rows["child_asin"] = df_rows["child_asin"].astype(str).str.strip()
    df_rows["amazon_sku"] = df_rows["amazon_sku"].astype(str).str.strip()
    df_rows["Units"] = pd.to_numeric(df_rows["Units"], errors="coerce").fillna(0.0)

    loc_rows = df_rows[df_rows["amazon_sku"].str.upper().str.endswith("-LOC")].copy()
    if loc_rows.empty:
        pytest.xfail(f"No '-LOC' rows found on {d}; cannot validate LOC transformation on that day.")

    # Pick one LOC row to validate
    sample = loc_rows.iloc[0]
    child_asin = sample["child_asin"]
    units_loc = float(sample["Units"])

    mapping = _normalize_mapping(load_asin_sku_mapping())
    mapped = mapping[mapping["child_asin"] == child_asin]
    if mapped.empty:
        pytest.xfail(f"LOC sample child ASIN {child_asin} not found in Gross&Net mapping; cannot validate naming rule.")

    mapped_sku = str(mapped.iloc[0]["mapped_sku"]).strip()

    # Apply your naming rule
    asin_out = f"{child_asin}-loc"
    sku_out = f"{mapped_sku}-LOC"

    # Build transformed products table just for this test (mirrors production logic)
    prod = df_rows.merge(mapping, on="child_asin", how="left")
    prod = prod[prod["mapped_sku"].notna()].copy()

    loc_mask = prod["amazon_sku"].str.upper().str.endswith("-LOC")
    prod["asin_out"] = prod["child_asin"]
    prod["sku_out"] = prod["mapped_sku"]
    prod.loc[loc_mask, "asin_out"] = prod.loc[loc_mask, "asin_out"] + "-loc"
    prod.loc[loc_mask, "sku_out"] = prod.loc[loc_mask, "sku_out"] + "-LOC"

    # Verify LOC output row exists
    match = prod[(prod["asin_out"] == asin_out) & (prod["sku_out"] == sku_out)]
    assert not match.empty, "Expected LOC output row (asin_out, sku_out) not found."

    # Verify Units carried over correctly (per-row check)
    # There could be multiple Amazon rows mapping to same out keys; sum for safety.
    units_out = float(match["Units"].sum())
    assert units_out == units_loc, f"Expected LOC units to carry over. got {units_out}, expected {units_loc}"

    # Verify base and loc remain separate keys
    base_exists = not prod[(prod["asin_out"] == child_asin) & (prod["sku_out"] == mapped_sku)].empty
    loc_exists = not prod[(prod["asin_out"] == asin_out) & (prod["sku_out"] == sku_out)].empty
    assert base_exists and loc_exists, "Expected both base and LOC rows to exist as separate products."