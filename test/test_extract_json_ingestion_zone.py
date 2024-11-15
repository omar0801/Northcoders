from moto import mock_aws
import json, pytest, boto3, logging
from src.extract_json_ingestion_zone import *
from pg8000.native import Connection
from unittest.mock import  patch



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

def test_connection_to_db():
    conn = connect_to_db()
    assert isinstance(conn, Connection)
    close_db_connection(conn)

@pytest.fixture()
def s3_mock():
    with mock_aws():
        s3 = boto3.client('s3', region_name='eu-west-2')
        
        s3.create_bucket(Bucket='test-bucket',
                        CreateBucketConfiguration={
                        'LocationConstraint': 'eu-west-2'}
                        )
        yield s3

@pytest.fixture()
def s3_mock_with_objects(s3_mock):
    for i in range(10):
        for table in tables:
            fake_timestamp = f'2024-11-14T09:27:40.35701{i}/{table}.json'
            s3_mock.put_object(Bucket='test-bucket',
                            Body=b'test_content',
                            Key=fake_timestamp)
    yield s3_mock

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

def generate_s3_event(bucket_name, object_key):
    return {
        'Records': [
            {
                's3': {
                    'bucket': {'name': bucket_name},
                    'object': {'key': object_key}
                    }
            }
            ]
        }
    
class TestFetchFromS3:
    def test_function_success(self, s3_mock, caplog):
        content = {'test': 'test_value'}
        s3_mock.put_object(
        Bucket='test-bucket',
        Key='test.json',
        Body=json.dumps(content)
        )
        with caplog.at_level(logging.INFO):
            response = fetch_from_s3('test-bucket', 'test.json',s3_mock)
        assert response == content
        assert "Lambda handler invoked with event" in caplog.text
        assert "Fetching object from S3" in caplog.text
        assert "Successfully retrieved and parsed object from S3" in caplog.text
        
    def test_object_not_found(self, s3_mock, caplog):
        with caplog.at_level(logging.INFO):
            response = fetch_from_s3('test-bucket', 'non-existent.json',s3_mock)
        assert "Object not found in S3" in caplog.text
        
    def test_invalid_json_format(self, s3_mock, caplog):
        s3_mock.put_object(
        Bucket='test-bucket',
        Key='test.json',
        Body='Not a json'
        )
        with caplog.at_level(logging.INFO):
            response = fetch_from_s3('test-bucket', 'test.json',s3_mock)
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



class TestLambdaHandler:

    expected_db_data = [
        {"currency_id": 1, "currency_code": "GBP", "created_at": "2022-11-03T14:20:49", "amount": 100.0},
        {"currency_id": 2, "currency_code": "USD", "created_at": "2022-11-03T14:20:49", "amount": 200.0}
    ]

    expected_s3_data = [
        {"currency_id": 1, "currency_code": "GBP", "created_at": "2022-11-03T14:20:49", "amount": 100.0},
        {"currency_id": 2, "currency_code": "USD", "created_at": "2022-11-03T14:20:49", "amount": 200.0}
    ]

    @patch('src.extract_json_ingestion_zone.fetch_from_s3')
    @patch('src.extract_json_ingestion_zone.fetch_data_from_table')
    @patch('src.extract_json_ingestion_zone.get_latest_s3_keys')
    @patch('src.extract_json_ingestion_zone.connect_to_db')
    def test_returns_dict(self, mock_connect_to_db, mock_get_latest_s3_keys, mock_fetch_data_from_table, mock_fetch_from_s3):

        mock_connect_to_db.return_value = MockConnection()
        mock_get_latest_s3_keys.return_value = 'latest_timestamp'
        mock_fetch_data_from_table.return_value = self.expected_db_data
        mock_fetch_from_s3.return_value = self.expected_s3_data

        result = lambda_handler(None, None)
        assert isinstance(result, dict)

    @patch('src.extract_json_ingestion_zone.fetch_from_s3')
    @patch('src.extract_json_ingestion_zone.fetch_data_from_table')
    @patch('src.extract_json_ingestion_zone.get_latest_s3_keys')
    @patch('src.extract_json_ingestion_zone.connect_to_db')
    def test_returned_dict_has_correct_keys(self, mock_connect_to_db, mock_get_latest_s3_keys, mock_fetch_data_from_table, mock_fetch_from_s3):

        mock_connect_to_db.return_value = MockConnection()
        mock_get_latest_s3_keys.return_value = 'latest_timestamp'
        mock_fetch_data_from_table.return_value = self.expected_db_data
        mock_fetch_from_s3.return_value = self.expected_s3_data

        result = lambda_handler(None, None)
        assert result.get('s3') != None
        assert result.get('db') != None

    @patch('src.extract_json_ingestion_zone.fetch_from_s3')
    @patch('src.extract_json_ingestion_zone.fetch_data_from_table')
    @patch('src.extract_json_ingestion_zone.get_latest_s3_keys')
    @patch('src.extract_json_ingestion_zone.connect_to_db')
    def test_values_are_dict(self, mock_connect_to_db, mock_get_latest_s3_keys, mock_fetch_data_from_table, mock_fetch_from_s3):

        mock_connect_to_db.return_value = MockConnection()
        mock_get_latest_s3_keys.return_value = 'latest_timestamp'
        mock_fetch_data_from_table.return_value = self.expected_db_data
        mock_fetch_from_s3.return_value = self.expected_s3_data

        result = lambda_handler(None, None)
        assert isinstance(result['db'], dict)
        assert isinstance(result['s3'], dict)

    @patch('src.extract_json_ingestion_zone.fetch_from_s3')
    @patch('src.extract_json_ingestion_zone.fetch_data_from_table')
    @patch('src.extract_json_ingestion_zone.get_latest_s3_keys')
    @patch('src.extract_json_ingestion_zone.connect_to_db')
    def test_inner_db_dict_have_correct_keys(self, mock_connect_to_db, mock_get_latest_s3_keys, mock_fetch_data_from_table, mock_fetch_from_s3):
    
        mock_connect_to_db.return_value = MockConnection()
        mock_get_latest_s3_keys.return_value = 'latest_timestamp'
        mock_fetch_data_from_table.return_value = self.expected_db_data
        mock_fetch_from_s3.return_value = self.expected_s3_data

        result = lambda_handler(None, None)
        for key in result['db'].keys():
            assert key in tables

    @patch('src.extract_json_ingestion_zone.fetch_from_s3')
    @patch('src.extract_json_ingestion_zone.fetch_data_from_table')
    @patch('src.extract_json_ingestion_zone.get_latest_s3_keys')
    @patch('src.extract_json_ingestion_zone.connect_to_db')
    def test_inner_db_dict_values_list_of_dicts(self, mock_connect_to_db, mock_get_latest_s3_keys, mock_fetch_data_from_table, mock_fetch_from_s3):

        mock_connect_to_db.return_value = MockConnection()
        mock_get_latest_s3_keys.return_value = 'latest_timestamp'
        mock_fetch_data_from_table.return_value = self.expected_db_data
        mock_fetch_from_s3.return_value = self.expected_s3_data

        result = lambda_handler(None, None)
        db = result['db']
        for item_list in db.values():
            assert isinstance(item_list, list)
            for item_dict in item_list:
                assert isinstance(item_dict, dict)

    @patch('src.extract_json_ingestion_zone.fetch_from_s3')
    @patch('src.extract_json_ingestion_zone.fetch_data_from_table')
    @patch('src.extract_json_ingestion_zone.get_latest_s3_keys')
    @patch('src.extract_json_ingestion_zone.connect_to_db')
    def test_db_dict_has_correct_values(self, mock_connect_to_db, mock_get_latest_s3_keys, mock_fetch_data_from_table, mock_fetch_from_s3):
        expected_data = self.expected_db_data

        mock_connect_to_db.return_value = MockConnection()
        mock_get_latest_s3_keys.return_value = 'latest_timestamp'
        mock_fetch_data_from_table.return_value = expected_data
        mock_fetch_from_s3.return_value = self.expected_s3_data 

        result = lambda_handler(None, None)
        db = result['db']
        for item in db.values():
            assert item == expected_data


class TestGetLatestS3Keys:
    #returns a list of strings
    #strings are valid s3 objects - mock
    #strings returned contain the latest timestamp

    def test_returns_string(self,s3_mock_with_objects):
        result = get_latest_s3_keys("test-bucket",s3_mock_with_objects)
        assert isinstance(result, str)

    def test_returned_strings_is_latest_timestamp(self, s3_mock_with_objects):
        result = get_latest_s3_keys('test-bucket',s3_mock_with_objects)
        expected_result = '2024-11-14T09:27:40.357019'
        assert result == expected_result
