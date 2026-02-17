import pandas as pd 

from weekly_summary.transform.gross_net_clean import clean_gross_net_df
from weekly_summary.transform.gross_net_select import select_gross_net_mapping

def test_gross_net_mapping_smoke():
    # Minimal realistic sample resembling Gross & Net after gsheets_to_df
    df = pd.DataFrame(
        {
            "SKU": ["A", "B", "", None, "A"], # includes whitespace, blanks, and duplicates
            "ASIN": ["X1", "Y1", "Z1", "W1", "X2"],
            "Mini SKU": ["mA", "mB", "mC", "mD", "mA2"],
            "Selling Price": ["$10.00", "20.50", "", "$5.00", "$99.00"],
        }
    )

    cleaned = clean_gross_net_df(df)
    mapping = select_gross_net_mapping(cleaned)

    # Required output colums
    assert list(mapping.columns) == ["SKU", "ASIN", "Mini SKU", "Selling Price"]
    assert (mapping["SKU"].astype(str).str.strip() != "").all()

    # SKU stripped
    assert mapping["SKU"].to_list()[0] == "A"

    # No duplicates after selector
    assert int(mapping["SKU"].duplicated().sum()) == 0

    # Selling Price should be numeric after cleaning 
    assert pd.api.types.is_numeric_dtype(mapping["Selling Price"])

