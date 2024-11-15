from src.extract_data import fetch_data_from_table,save_to_json,main, create_s3_bucket, save_to_s3
from datetime import datetime
from decimal import Decimal
from unittest.mock import patch , MagicMock
import pytest
from moto import mock_aws
import boto3
from pprint import pprint
import json

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
            messages = main({}, {}, fetch_func=mock_fetch, save_func=mock_save)
            mock_connect.assert_called_once()
            mock_close_db.assert_called_once_with(mock_connect.return_value)
        
            assert mock_fetch.call_count == len(test_tables)
            assert mock_save.call_count == len(test_tables)
            assert messages[0] == f"Extracting data from {test_tables[0]}..."
            assert messages[1] == "Mock save successful"
            assert messages[-1] == "Database connection closed."

@pytest.fixture()
def s3_mock():
    with mock_aws():
        s3 = boto3.client('s3')
        yield s3

class TestCreateS3():
    #creates bucket - bucket exists in mock_aws
    #can create two buckets with same prefix
    #returned bucket name is correct
    #will modify prefix containing capital
    #will modify prefix containing '.' or ':'
    #would raise exception if invalid prefix given e.g. contains question mark


    def test_creates_bucket(self, s3_mock):
        bucket_prefix = 'ingestion-bucket-'
        create_s3_bucket(bucket_prefix, s3_mock)
        response = s3_mock.list_buckets(Prefix=bucket_prefix)
        assert len(response['Buckets']) == 1

    def test_creates_two_buckets_with_same_prefix(self, s3_mock):
        bucket_prefix = 'ingestion-bucket-'
        create_s3_bucket(bucket_prefix, s3_mock)
        create_s3_bucket(bucket_prefix, s3_mock)
        response = s3_mock.list_buckets(Prefix=bucket_prefix)
        assert len(response['Buckets']) == 2


    def test_capitals_are_corrected_in_prefix(self, s3_mock):
        bucket_prefix = 'Ingestion-bucket-'
        create_s3_bucket(bucket_prefix, s3_mock)

        response = s3_mock.list_buckets(Prefix=bucket_prefix) #case insensitive
        assert response['Buckets'][0]['Name'][0:17] == 'ingestion-bucket-'

    def test_full_stops_removed_from_prefix(self, s3_mock):
        bucket_prefix = 'i.ngestion-bucket-'
        create_s3_bucket(bucket_prefix, s3_mock)

        response = s3_mock.list_buckets(Prefix=bucket_prefix)
        assert response['Buckets'][0]['Name'][0:17] == 'ingestion-bucket-'

    def test_colon_removed_from_prefix(self, s3_mock):
        bucket_prefix = 'i:ngestion-bucket-'
        create_s3_bucket(bucket_prefix, s3_mock)

        response = s3_mock.list_buckets(Prefix=bucket_prefix)
        assert response['Buckets'][0]['Name'][0:17] == 'ingestion-bucket-'

    def test_returned_bucket_name_exists(self, s3_mock):
        bucket_prefix = 'Ingestion-bucket-'
        bucket_name = create_s3_bucket(bucket_prefix, s3_mock)

        response = s3_mock.list_buckets()
        assert response['Buckets'][0]['Name'] == bucket_name

    def test_returns_error_if_invalid_prefix_given(self, s3_mock):
        bucket_prefix = '?Ingestion-bucket-'
        response = create_s3_bucket(bucket_prefix, s3_mock)

        assert response == 'An error has occured'


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

class TestPutObject():

    def test_creates_object(self, s3_mock):
        bucket_name = create_s3_bucket('test_bucket', s3_mock)
        save_to_s3(expected_data, bucket_name, 'test_object', s3_mock)

        object_list = s3_mock.list_objects_v2(Bucket=bucket_name)
        assert object_list['Contents'][0]['Key'] == 'test_object'

    def test_object_body_as_expected(self, s3_mock):
        bucket_name = create_s3_bucket('test_bucket', s3_mock)
        save_to_s3(expected_data, bucket_name, 'test_object', s3_mock)

        result = s3_mock.get_object(
            Bucket=bucket_name,
            Key='test_object'
        )['Body'].read()

        assert json.loads(result) == expected_data