import pandas as pd
import pytest

from weekly_summary.transform.master_carton_select import (MasterCartonParseError, select_master_carton_mapping, )

def test_select_master_carton_mapping_happy_path():
    df = pd.DataFrame(
        {
            "Mini SKU:": ["MA-01", " MA-02 ", ""],
            "Qty per Master:": [1, 2, 3],
            "Other": ["x", "y", "z"],
        }
    )

    out = select_master_carton_mapping(df)

    assert list(out.columns) == ["Mini SKU", "Qty per Master"]
    assert out.to_dict(orient="records") == [
        {"Mini SKU": "MA-01", "Qty per Master": 1}, 
        {"Mini SKU": "MA-02", "Qty per Master": 2},
    ]

def test_missing_required_columns_raises():
    df = pd.DataFrame({"Mini SKU:": ["MA-01"]})
    with pytest.raises(MasterCartonParseError):
        select_master_carton_mapping(df)


def test_qty_per_master_must_be_numeric():
    df = pd.DataFrame({"Mini SKU:": ["MA-01"], "Qty per Master:": ["not a number"]})
    out = select_master_carton_mapping(df)
    assert out.shape == (0, 2)

def test_duplicates_keep_last():
    df = pd.DataFrame(
        {"Mini SKU:": ["MA-01", "MA-01"], "Qty per Master:": [1, 5]}
    )

    out = select_master_carton_mapping(df)
    assert out.to_dict(orient="records") == [{"Mini SKU": "MA-01", "Qty per Master": 5}]
    

