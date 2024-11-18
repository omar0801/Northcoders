from src.ingestion import *
from datetime import datetime
from decimal import Decimal
from unittest.mock import patch , Mock
import pytest
from moto import mock_aws
import boto3
from pprint import pprint
import json
from pg8000.native import Connection

tables = [
    "counterparty",
    "currency",
    "department",
    "design",
    "staff",
    "sales_order",
    "address",
    "payment",
    "purchase_order",
    "payment_type",
    "transaction"
]

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

@pytest.fixture()
def s3_mock():
    with mock_aws():
        s3 = boto3.client('s3')
        yield s3

@pytest.fixture()
def s3_mock_with_bucket():
    with mock_aws():
        s3 = boto3.client('s3', region_name='eu-west-2')
        
        s3.create_bucket(Bucket='ingestion-bucket-neural-normalisers-new',
                        CreateBucketConfiguration={
                        'LocationConstraint': 'eu-west-2'}
                        )
        yield s3

@pytest.fixture()
def s3_mock_with_objects(s3_mock_with_bucket):
    for i in range(10):
        for table in tables:
            fake_timestamp = f'{table}/2024-11-14T09:27:40.35701{i}.json'
            s3_mock_with_bucket.put_object(Bucket='ingestion-bucket-neural-normalisers-new',
                            Body=b'test_content',
                            Key=fake_timestamp)
    yield s3_mock_with_bucket

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
    
    def test_creates_object(self, s3_mock_with_bucket):
        save_to_s3(expected_data, 'ingestion-bucket-neural-normalisers-new', 'test_object', s3_mock_with_bucket)

        object_list = s3_mock_with_bucket.list_objects_v2(Bucket='ingestion-bucket-neural-normalisers-new')
        assert object_list['Contents'][0]['Key'] == 'test_object'

    def test_object_body_as_expected(self, s3_mock_with_bucket):
        save_to_s3(expected_data, 'ingestion-bucket-neural-normalisers-new', 'test_object', s3_mock_with_bucket)

        result = s3_mock_with_bucket.get_object(
            Bucket='ingestion-bucket-neural-normalisers-new',
            Key='test_object'
        )['Body'].read()

        assert json.loads(result) == expected_data

def test_connection_to_db():
    conn = connect_to_db()
    assert isinstance(conn, Connection)
    close_db_connection(conn)
    
class TestFetchFromS3:
    def test_function_success(self, s3_mock_with_bucket, caplog):
        content = {'test': 'test_value'}
        s3_mock_with_bucket.put_object(
        Bucket='ingestion-bucket-neural-normalisers-new',
        Key='test.json',
        Body=json.dumps(content)
        )
        with caplog.at_level(logging.INFO):
            response = fetch_from_s3('ingestion-bucket-neural-normalisers-new', 'test.json', s3_mock_with_bucket)
        assert response == content
        assert "Lambda handler invoked with event" in caplog.text
        assert "Fetching object from S3" in caplog.text
        assert "Successfully retrieved and parsed object from S3" in caplog.text
        
    def test_object_not_found(self, s3_mock_with_bucket, caplog):
        with caplog.at_level(logging.INFO):
            response = fetch_from_s3('ingestion-bucket-neural-normalisers-new', 'non-existent.json', s3_mock_with_bucket)
        assert "Object not found in S3" in caplog.text
        
    def test_invalid_json_format(self, s3_mock_with_bucket, caplog):
        s3_mock_with_bucket.put_object(
        Bucket='ingestion-bucket-neural-normalisers-new',
        Key='test.json',
        Body='Not a json'
        )
        with caplog.at_level(logging.INFO):
            response = fetch_from_s3('ingestion-bucket-neural-normalisers-new', 'test.json', s3_mock_with_bucket)
        assert "Failed to decode JSON" in caplog.text

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

class TestGetLatestS3Keys:
    #returns a list of strings
    #strings are valid s3 objects - mock
    #strings returned contain the latest timestamp

    def test_returns_string(self, s3_mock_with_objects):
        result = get_latest_s3_keys('ingestion-bucket-neural-normalisers-new', s3_mock_with_objects,'currency')
        assert isinstance(result, str)

    def test_returned_strings_is_latest_timestamp(self, s3_mock_with_objects):
        result = get_latest_s3_keys('ingestion-bucket-neural-normalisers-new', s3_mock_with_objects, 'currency')
        expected_result = '2024-11-14T09:27:40.357019'
        assert result == expected_result

class TestLambdaHandler:

    expected_db_data = [
        {"currency_id": 1, "currency_code": "GBP", "created_at": "2022-11-03T14:20:49", "amount": 100.0},
        {"currency_id": 2, "currency_code": "USD", "created_at": "2022-11-03T14:20:49", "amount": 200.0}
    ]

    expected_s3_data = [
        {"currency_id": 1, "currency_code": "GBP", "created_at": "2022-11-03T14:20:49", "amount": 100.0},
        {"currency_id": 2, "currency_code": "USD", "created_at": "2022-11-03T14:20:49", "amount": 200.0},
        {"currency_id": 3, "currency_code": "EUR", "created_at": "2022-11-03T14:20:49", "amount": 200.0}
    ]

    @patch('src.ingestion.fetch_from_s3')
    @patch('src.ingestion.fetch_data_from_table')
    @patch('src.ingestion.get_latest_s3_keys')
    @patch('src.ingestion.connect_to_db')
    @patch('src.ingestion.datetime')
    @patch('src.ingestion.main_check_for_changes')
    def test_lambda_handler_correct_s3_files_created(self,
                                                mock_check_for_changes,
                                                mock_date_time,
                                                mock_connect_to_db,
                                                mock_get_latest_s3_keys,
                                                mock_fetch_data_from_table,
                                                mock_fetch_from_s3,    
                                                s3_mock_with_bucket):

        mock_connect_to_db.return_value = MockConnection()
        mock_get_latest_s3_keys.return_value = 'latest_timestamp'
        mock_fetch_data_from_table.return_value = self.expected_db_data
        mock_fetch_from_s3.return_value = self.expected_s3_data
        mock_check_for_changes.return_value = ['currency']
        mock_date_time.now().isoformat = Mock(return_value='latest_timestamp')   

        lambda_handler(None, None)

        s3_object = s3_mock_with_bucket.get_object(
            Bucket='ingestion-bucket-neural-normalisers-new',
            Key='currency/latest_timestamp.json'
        )

        body = s3_object['Body'].read().decode()
        body = json.loads(body)

        assert body == self.expected_db_data



