from src.process_data import *
from datetime import datetime
from decimal import Decimal
from unittest.mock import patch , Mock
import pytest
from moto import mock_aws
import boto3
from pprint import pprint
import json

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
        fake_timestamp = f'changes_log/sales_order/2024-11-14T09:27:40.35702{i}.json'
        s3_mock_with_bucket.put_object(Bucket='ingestion-bucket-neural-normalisers-new',
                            Body=b'test_content',
                            Key=fake_timestamp)
        for table in tables:
            fake_timestamp = f'{table}/2024-11-14T09:27:40.35701{i}.json'
            s3_mock_with_bucket.put_object(Bucket='ingestion-bucket-neural-normalisers-new',
                            Body=b'test_content',
                            Key=fake_timestamp)
            
    yield s3_mock_with_bucket


class TestGetLatestS3Keys:
    #returns a list of strings
    #strings are valid s3 objects - mock
    #strings returned contain the latest timestamp

    def test_returns_string(self, s3_mock_with_objects):
        result = get_latest_s3_keys('ingestion-bucket-neural-normalisers-new', s3_mock_with_objects,'currency')
        assert isinstance(result, str)

    def test_returned_strings_is_latest_timestamp(self, s3_mock_with_objects):
        result = get_latest_s3_keys('ingestion-bucket-neural-normalisers-new', s3_mock_with_objects, 'sales_order')
        expected_result = '2024-11-14T09:27:40.357019'
        assert result == expected_result

    def test_returned_strings_is_latest_timestamp_from_changed_log(self, s3_mock_with_objects):
        result = get_latest_s3_keys('ingestion-bucket-neural-normalisers-new', s3_mock_with_objects, 'changes_log/sales_order')
        expected_result = '2024-11-14T09:27:40.357029'
        assert result == expected_result

@pytest.fixture()
def s3_mock_with_processed_bucket():
    with mock_aws():
        s3 = boto3.client('s3', region_name='eu-west-2')
        s3.create_bucket(Bucket='processed-bucket-neural-normalisers',
                        CreateBucketConfiguration={
                        'LocationConstraint': 'eu-west-2'}
)
        yield s3

@pytest.fixture()
def s3_mock_with_processed_bucket_objects(s3_mock_with_processed_bucket):
    s3_mock_with_processed_bucket.put_object(Bucket='processed-bucket-neural-normalisers',
                            Body=b'test_content',
                            Key="processed_data/facts_sales/2024-11-14T09:27:40.357029.parquet")
    yield s3_mock_with_processed_bucket

    

class TestSalesParquet:
    #test function returns 0 if no facts_sales parquet found
    #test to return none zewro integer if any found (fact_sales parquet)

    
    def test_check_for_sales_parquet_returns_zero(self, s3_mock_with_processed_bucket):
        result = check_for_fact_sales_parquet(s3_mock_with_processed_bucket)
        assert result == 0

    def test_check_for_sales_parquet_returns_integer(self, s3_mock_with_processed_bucket_objects):
        result = check_for_fact_sales_parquet(s3_mock_with_processed_bucket_objects)
        assert result == 1




