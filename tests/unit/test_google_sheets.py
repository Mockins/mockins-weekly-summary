from weekly_summary.extract.google_sheets import read_range


class DummyService:
    def spreadsheets(self):
        return self

    def values(self):
        return self

    def get(self, spreadsheetId, range):
        return self

    def execute(self):
        return {"values": [["SKU", "Price"], ["ABC", "10.00"]]}


def test_read_range_returns_values():
    service = DummyService()
    values = read_range(service, "dummy_sheet_id", "AMZ US!A1:B2")
    assert values == [["SKU", "Price"], ["ABC", "10.00"]]
