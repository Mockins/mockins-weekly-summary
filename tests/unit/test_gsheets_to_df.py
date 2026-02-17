import pandas as pd
import pytest

from weekly_summary.transform.gsheets_to_df import GSheetsParseError, values_to_dataframe
def test_empty_values_raises():
    with pytest.raises(GSheetsParseError):
        values_to_dataframe([])


def test_header_only_returns_empty_df_with_columns():
    df = values_to_dataframe([["SKU", "Price"]])
    assert list(df.columns) == ["SKU", "Price"]
    assert df.shape == (0, 2)


def test_pads_short_rows_to_header_length():
    values = [
        ["A", "B", "C"], 
        ["1", "2"], 
    ]
    df = values_to_dataframe(values)
    assert df.iloc[0].tolist() == ["1", "2", ""]

def test_truncates_long_rows_to_header_length():
    values  = [
        ["A", "B"],
        ["1", "2", "3", "4"],
    ]
    df = values_to_dataframe(values)
    assert df.iloc[0].tolist() == ["1", "2"]

def test_duplicate_headers_are_made_unique():
    values = [
        ["Cost", "Cost", "Cost"],
        ["1", "2", "3"],
    ]

    df = values_to_dataframe(values)

    assert list(df.columns) == ["Cost", "Cost__1", "Cost__2"]


def test_empty_header_cell_is_dropped():
    values = [
        ["SKU", "", "Price"],
        ["A1", "ignored", "10.00"],
    ]

    df = values_to_dataframe(values)

    assert list(df.columns) == ["SKU", "Price"]
    assert df.shape == (1, 2)


        