import pandas as pd
import pytest

from weekly_summary.transform.gross_net_select import select_gross_net_mapping

def test_raises_if_required_columns_missing():
    df = pd.DataFrame({"SKU": ["A"]})
    with pytest.raises(ValueError):
        select_gross_net_mapping(df)

def test_drops_blank_sku_rows_and_strips():
    df = pd.DataFrame(
        {
            "SKU": ["  A  ", "", None],
            "ASIN": ["X", "Y", "Z"], 
            "Mini SKU": ["m1", "m2", "m3"],
            "Selling Price": [10.0, 20.0, 30.0],
        }
    )
    out = select_gross_net_mapping(df)
    assert out["SKU"].tolist() == ["A"]
    assert out.shape == (1, 4)


def test_duplicates_by_keeps_first():
    df = pd.DataFrame(
        {
            "SKU": ["A", "A", "B"],
            "ASIN": ["X1", "X2", "Y"],
            "Mini SKU": ["m1", "m2", "m3"],
            "Selling Price": [10.0, 99.0, 20.0],
        }
    )
    out = select_gross_net_mapping(df)
    assert out["SKU"].tolist() == ["A", "B"]

    # Keeps first row A row
    assert out.loc[out["SKU"] == "A", "ASIN"].iloc[0] == "X1"
                   

