from src.process_data import create_dim_location
import pytest
import json
import pandas as pd
from moto import mock_aws
import boto3
from unittest.mock import patch



@pytest.fixture
def records():
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
    return address_data

class TestCreateDimLocation:    
    def test_dataframe_contains_correct_columns(self, records):
        output = create_dim_location(records)
        column_names = list(output['dataframe'].columns)
        assert len(column_names) == 8
        assert 'location_id' in column_names
        assert 'address_line_1' in column_names
        assert 'address_line_2' in column_names
        assert 'district' in column_names
        assert 'city' in column_names
        assert 'postal_code' in column_names
        assert 'country' in column_names
        assert 'phone' in column_names

    def test_dataframe_contains_correct_values(self, records):
        output = create_dim_location(records)
        output = output['dataframe'].map(lambda x: None if pd.isna(x) else x)
        for i in range(len(records)):
            row_values = output.iloc[i]
            output_values = row_values.tolist()
            original_values = list(records[i].values())
            assert len(output_values) == len(original_values) - 2
            assert output_values[0] == original_values[0]
            assert output_values[1:8] == original_values[1:8]