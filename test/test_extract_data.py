from src.extract_data import fetch_data_from_table,save_to_json,main
from datetime import datetime
from decimal import Decimal
from unittest.mock import patch , MagicMock
import pytest

class MockConnection:
    def run(self, query):
        return [
            (1, "GBP", datetime(2022, 11, 3, 14, 20, 49), Decimal("100.0")),
            (2, "USD", datetime(2022, 11, 3, 14, 20, 49), Decimal("200.0"))
        ]
    @property
    def columns(self):
        return [
            {"name": "currency_id"},
            {"name": "currency_code"},
            {"name": "created_at"},
            {"name": "amount"}
        ]

@pytest.fixture
def mock_conn():
    return MockConnection()


class TestFetchData:
    def test_fetch_data_from_table(self,mock_conn):
        table_name = "currency"
        data = fetch_data_from_table(mock_conn, table_name)
        
        expected_data = [
            {
                "currency_id": 1,
                "currency_code": "GBP",
                "created_at": "2022-11-03T14:20:49",
                "amount": 100.0
            },
            {
                "currency_id": 2,
                "currency_code": "USD",
                "created_at": "2022-11-03T14:20:49",
                "amount": 200.0
            }
        ]
        assert data == expected_data

    def test_datetime_serialisation(self, mock_conn):
        data = fetch_data_from_table(mock_conn, "currency")
        for entry in data:
            assert isinstance(entry["created_at"], str)
        
    def test_decimal_serialisation(self, mock_conn):
        data = fetch_data_from_table(mock_conn, "currency")
        for entry in data:
            assert isinstance(entry["amount"],float)

class TestSaveToJson:
    def test_save_to_json(self):
        data = [
            {
                "currency_id": 1,
                "currency_code": "GBP",
                "created_at": "2022-11-03T14:20:49",
                "amount": 100.0
            },
            {
                "currency_id": 2,
                "currency_code": "USD",
                "created_at": "2022-11-03T14:20:49",
                "amount": 200.0
            }
        ]

        filename = "currency.json"

        result = save_to_json(data, filename)
        expected_message = f"File '{filename}' has been saved successfully in the 'data' directory."
        assert result == expected_message

test_tables = [
    "test_table_1",
    "test_table_2",
    "test_table_3"
]

class TestMain:
    @patch("src.extract_data.close_db_connection")
    @patch("src.extract_data.connect_to_db")
    def test_main_with_mocks(self, mock_connect, mock_close_db):
        mock_fetch = MagicMock(return_value=[{"id": 1, "name": "Sample"}])
        mock_save = MagicMock(return_value="Mock save successful")

        with patch("src.extract_data.tables", test_tables):
            messages = main(fetch_func=mock_fetch, save_func=mock_save)
            mock_connect.assert_called_once()
            mock_close_db.assert_called_once_with(mock_connect.return_value)
        
            assert mock_fetch.call_count == len(test_tables)
            assert mock_save.call_count == len(test_tables)
            assert messages[0] == f"Extracting data from {test_tables[0]}..."
            assert messages[1] == "Mock save successful"
            assert messages[-1] == "Database connection closed."