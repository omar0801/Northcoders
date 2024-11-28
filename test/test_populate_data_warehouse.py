from src.populate_data_warehouse import *
import pytest
from unittest.mock import patch
from moto import mock_aws
from io import BytesIO
import pandas as pd
from pg8000.native import Connection


def test_connection_to_db():
    conn = connect_to_db()
    assert isinstance(conn, Connection)
    close_db_connection(conn)

@pytest.fixture()
def s3_mock():
    with mock_aws():
        s3 = boto3.client('s3')
        yield s3

@pytest.fixture()
def s3_mock_with_bucket():
    with mock_aws():
        s3 = boto3.client('s3', region_name='eu-west-2')
        
        s3.create_bucket(Bucket='processed-bucket-neural-normalisers',
                        CreateBucketConfiguration={
                        'LocationConstraint': 'eu-west-2'}
                        )
        yield s3

@pytest.fixture()
def s3_mock_with_objects(s3_mock_with_bucket):
    for i in range(10):
        for table in tables:
            fake_timestamp = f'processed_data/{table}/2024-11-14T09:27:40.35701{i}.parquet'
            s3_mock_with_bucket.put_object(Bucket='processed-bucket-neural-normalisers',
                            Body=b'test_content',
                            Key=fake_timestamp)
    yield s3_mock_with_bucket

class TestGetLatestS3Keys:

    def test_returns_string(self, s3_mock_with_objects):
        result = get_latest_s3_keys('processed-bucket-neural-normalisers', s3_mock_with_objects,'dim_currency')
        assert isinstance(result, list)

    def test_returned_strings_is_latest_timestamp(self, s3_mock_with_objects):
        result = get_latest_s3_keys('processed-bucket-neural-normalisers', s3_mock_with_objects, 'dim_currency')
        expected_result = ['2024-11-14T09:27:40.357019', '2024-11-14T09:27:40.357018']
        assert result == expected_result

class TestFetchFromS3:
    def test_function_success(self, s3_mock_with_bucket):
        df = pd.DataFrame({'test': ['test_value']})
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")

        s3_mock_with_bucket.put_object(
        Bucket='processed-bucket-neural-normalisers',
        Key='test.parquet',
        Body=buffer.getvalue()
        )
        
        response = fetch_from_s3('processed-bucket-neural-normalisers', 'test.parquet', s3_mock_with_bucket)
        # Check that the fetched DataFrame has the same shape, columns, and index as expected.
        pd.testing.assert_frame_equal(response, df)

class TestLambdaHandler:
    def test_returns_none(self):
        assert lambda_handler(None, None) == None