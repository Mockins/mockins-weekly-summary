import pandas as pd
import pytest

from weekly_summary.transform.attach_master_carton_qty import (
    attach_master_carton_qty,
    MasterCartonMappingError,
)

def test_attach_qty_per_master_success():
    gross_net = pd.DataFrame(
        {
            "Mini SKU": ["MA-00", "MA-01", "MA-02"],
            "ASIN": ["A", "B", "C"],
            "Selling Price": [10.0, 20.0, 30.0],
        }
    )

    # Master Carton sheet often has colons in headers (like your real output)
    master_carton = pd.DataFrame(
        {
            "Mini SKU:": ["MA-00", "MA-01", "MA-02"],
            "Qty per Master:": [2, 1, 4],
        }
    )

    out = attach_master_carton_qty(gross_net, master_carton)

    assert "qty_per_master" in out.columns
    assert out["qty_per_master"].tolist() == [2, 1, 4]

def test_duplicate_mini_sku_in_master_carton_raises():
    gross_net = pd.DataFrame({"Mini SKU": ["MA-00", "MA-01"]})

    master_carton = pd.DataFrame(
        {
            "Mini SKU:": ["MA-00", "MA-00"],  # duplicate
            "Qty per Master:": [2, 3], 
        }
    )

    with pytest.raises(MasterCartonMappingError):
        attach_master_carton_qty(gross_net, master_carton)


def test_missing_required_columns_raises():
    gross_net = pd.DataFrame({"Mini SKU": ["MA-00"]})
    master_carton = pd.DataFrame({"Something Else": ["MA-00"]})

    with pytest.raises(MasterCartonMappingError):
        attach_master_carton_qty(gross_net, master_carton)

