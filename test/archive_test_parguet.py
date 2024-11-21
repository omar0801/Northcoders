from src.process_data import write_dataframe_to_s3
import pytest
import json
import pandas as pd
from moto import mock_aws
import boto3
from unittest.mock import patch



@pytest.fixture()
def s3_mock():
    with mock_aws():
        s3 = boto3.client('s3')
        s3.create_bucket(
            Bucket="processed-bucket-neural-normalisers",
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            }
        )
        yield s3

@pytest.fixture()
def datetime_mock():
    with patch('src.process_data.datetime') as mock_dt:
        mock_dt.now.return_value.isoformat.return_value = ('mock_timestamp')
        yield mock_dt

@pytest.fixture
def test_dataframe():
    address_data = [
      {
            "address_id": 1,
            "address_line_1": "6826 Herzog Via",
            "address_line_2": None,
            "district": "Avon",
            "city": "New Patienceburgh",
            "postal_code": "28441",
            "country": "Turkey",
            "phone": "1803 637401",
            "created_at": "2022-11-03T14:20:49.962000",
            "last_updated": "2022-11-03T14:20:49.962000"
      },
      {
            "address_id": 2,
            "address_line_1": "179 Alexie Cliffs",
            "address_line_2": None,
            "district": None,
            "city": "Aliso Viejo",
            "postal_code": "99305-7380",
            "country": "San Marino",
            "phone": "9621 880720",
            "created_at": "2022-11-03T14:20:49.962000",
            "last_updated": "2022-11-03T14:20:49.962000"
      }]
    
    output = pd.DataFrame(address_data)
    yield output

class TestWriteDataframeToS3:
    def test_parquet_file_uploaded_to_s3_bucket(self, s3_mock, test_dataframe, datetime_mock ):
        test_df_dict = {'dataframe': test_dataframe, 'table_name': 'dim_location'}
        write_dataframe_to_s3(test_df_dict, s3_mock)
        s3_contents = s3_mock.list_objects_v2(
                Bucket="processed-bucket-neural-normalisers"
            )
        assert s3_contents['Contents'][0]['Key'] == 'processed_data/dim_location/mock_timestamp.parquet'