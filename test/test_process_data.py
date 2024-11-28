from src.process_data import *
import pytest
from unittest.mock import patch
from moto import mock_aws
import io, datetime


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
        s3.create_bucket(
            Bucket='processed-bucket-neural-normalisers',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            }
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

class TestGetLatestS3Keys:

    def test_returns_string(self, s3_mock_with_objects):
        result = get_latest_s3_keys('ingestion-bucket-neural-normalisers-new', s3_mock_with_objects,'currency')
        assert isinstance(result, str)

    def test_returned_strings_is_latest_timestamp(self, s3_mock_with_objects):
        result = get_latest_s3_keys('ingestion-bucket-neural-normalisers-new', s3_mock_with_objects, 'currency')
        expected_result = '2024-11-14T09:27:40.357019'
        assert result == expected_result


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


@pytest.fixture()
def s3_processed_mock():
    with mock_aws():
        s3 = boto3.client('s3')
        s3.create_bucket(
            Bucket='processed-bucket-neural-normalisers',
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
    def test_parquet_file_uploaded_to_s3_bucket(self, s3_processed_mock, test_dataframe, datetime_mock ):
        test_df_dict = {'dataframe': test_dataframe, 'table_name': 'dim_location'}
        write_dataframe_to_s3(test_df_dict, s3_processed_mock)
        s3_contents = s3_processed_mock.list_objects_v2(
                Bucket='processed-bucket-neural-normalisers'
            )
        assert s3_contents['Contents'][0]['Key'] == 'processed_data/dim_location/mock_timestamp.parquet'


@pytest.fixture()
def mock_get_latest_s3_keys():
    with patch('src.process_data.get_latest_s3_keys') as latest_timestamp:
        latest_timestamp.return_value = ('latest_timestamp')

combined_data = [[
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
      }, {
    "address_id": 5,
    "address_line_1": "34177 Upton Track",
    "address_line_2": None,
    "district": None,
    "city": "Fort Shadburgh",
    "postal_code": "55993-8850",
    "country": "Bosnia and Herzegovina",
    "phone": "0081 009772",
    "created_at": "2022-11-03T14:20:49.962000",
    "last_updated": "2022-11-03T14:20:49.962000"
  }], [{
    "design_id": 8,
    "created_at": "2022-11-03T14:20:49.962000",
    "design_name": "Wooden",
    "file_location": "/usr",
    "file_name": "wooden-20220717-npgz.json",
    "last_updated": "2022-11-03T14:20:49.962000"
  },
  {
    "design_id": 51,
    "created_at": "2023-01-12T18:50:09.935000",
    "design_name": "Bronze",
    "file_location": "/private",
    "file_name": "bronze-20221024-4dds.json",
    "last_updated": "2023-01-12T18:50:09.935000"
  }], [
        {"currency_id": 1, "currency_code": "GBP", "created_at": "2022-11-03T14:20:49", "amount": 100.0},
        {"currency_id": 2, "currency_code": "USD", "created_at": "2022-11-03T14:20:49", "amount": 200.0},
        {"currency_id": 3, "currency_code": "EUR", "created_at": "2022-11-03T14:20:49", "amount": 200.0}
    ],
[{
    "counterparty_id": 3,
    "counterparty_legal_name": "Armstrong Inc",
    "legal_address_id": 2,
    "commercial_contact": "Jane Wiza",
    "delivery_contact": "Myra Kovacek",
    "created_at": "2022-11-03T14:20:51.563000",
    "last_updated": "2022-11-03T14:20:51.563000"
  },   {
    "counterparty_id": 9,
    "counterparty_legal_name": "Price LLC",
    "legal_address_id": 5,
    "commercial_contact": "Sheryl Langworth",
    "delivery_contact": "Simon Schoen",
    "created_at": "2022-11-03T14:20:51.563000",
    "last_updated": "2022-11-03T14:20:51.563000"
  }], [{"staff_id": 1, "first_name": "Jeremie", "last_name": "Franey", "department_id": 2, "email_address": "jeremie.franey@terrifictotes.com", "created_at": "2022-11-03T14:20:51.563000", "last_updated": "2022-11-03T14:20:51.563000"}, {"staff_id": 2, "first_name": "Deron", "last_name": "Beier", "department_id": 6, "email_address": "deron.beier@terrifictotes.com", "created_at": "2022-11-03T14:20:51.563000", "last_updated": "2022-11-03T14:20:51.563000"}], [{"department_id": 2, "department_name": "Purchasing", "location": "Manchester", "manager": "Naomi Lapaglia", "created_at": "2022-11-03T14:20:49.962000", "last_updated": "2022-11-03T14:20:49.962000"},  {
    "department_id": 6,
    "department_name": "Facilities",
    "location": "Manchester",
    "manager": "Shelley Levene",
    "created_at": "2022-11-03T14:20:49.962000",
    "last_updated": "2022-11-03T14:20:49.962000"
  }], [{"sales_order_id": 11293, "created_at": "2024-11-21T18:22:10.134000", "last_updated": "2024-11-21T18:22:10.134000",
        "design_id": 325, "staff_id": 13, "counterparty_id": 14, "units_sold": 41794, "unit_price": 2.08, "currency_id": 2, "agreed_delivery_date": "2024-11-24", "agreed_payment_date": "2024-11-26", "agreed_delivery_location_id": 2}, {"sales_order_id": 11294, "created_at": "2024-11-21T18:27:10.106000", "last_updated": "2024-11-21T18:27:10.106000", "design_id": 124, "staff_id": 1, "counterparty_id": 9, "units_sold": 87640, "unit_price": 3.41, "currency_id": 2, "agreed_delivery_date": "2024-11-27", "agreed_payment_date": "2024-11-27", "agreed_delivery_location_id": 28}], {"sales_order": {"additions": None, "deletions": [11293], "changes": None}}]

@pytest.fixture()
def mock_fetch_from_s3():
    with patch('src.process_data.fetch_from_s3') as mock_fetch:
        mock_fetch.side_effect = combined_data
        yield mock_fetch


class TestLambdaHandler():
    def test_creates_dim_parquet_files(self, s3_mock_with_objects, mock_get_latest_s3_keys, mock_fetch_from_s3, datetime_mock):
        lambda_handler(None, None)

        s3_contents = s3_mock_with_objects.list_objects_v2(
                Bucket='processed-bucket-neural-normalisers'
            )
        assert s3_contents['Contents'][0]['Key'] == 'processed_data/dim_counterparty/mock_timestamp.parquet'
        assert s3_contents['Contents'][1]['Key'] == 'processed_data/dim_currency/mock_timestamp.parquet'
        assert s3_contents['Contents'][2]['Key'] == 'processed_data/dim_date/mock_timestamp.parquet'
        assert s3_contents['Contents'][3]['Key'] == 'processed_data/dim_design/mock_timestamp.parquet'
        assert s3_contents['Contents'][4]['Key'] == 'processed_data/dim_location/mock_timestamp.parquet'
        assert s3_contents['Contents'][5]['Key'] == 'processed_data/dim_staff/mock_timestamp.parquet'

    def test_check_dim_currency_contains_correct_data(self, s3_mock_with_objects, mock_get_latest_s3_keys, mock_fetch_from_s3, datetime_mock):
        lambda_handler(None, None)

        currency_obj = s3_mock_with_objects.get_object(Bucket='processed-bucket-neural-normalisers',
            Key='processed_data/dim_currency/mock_timestamp.parquet')
        
        df = pd.read_parquet(io.BytesIO(currency_obj['Body'].read()))

        assert list(df.columns.values) ==  ['currency_id',
                                            'currency_code',
                                            'currency_name',
                                           ]
        assert df.loc[0].to_dict() == {"currency_id": 1, "currency_code": "GBP", "currency_name": "Great British Pounds"}

    def test_check_dim_location_contains_correct_data(self, s3_mock_with_objects, mock_get_latest_s3_keys, mock_fetch_from_s3, datetime_mock):
        lambda_handler(None, None)

        obj = s3_mock_with_objects.get_object(Bucket='processed-bucket-neural-normalisers',
            Key='processed_data/dim_location/mock_timestamp.parquet')
        
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

        column_names = list(df.columns)
        assert len(column_names) == 8
        assert 'location_id' in column_names
        assert 'address_line_1' in column_names
        assert 'address_line_2' in column_names
        assert 'district' in column_names
        assert 'city' in column_names
        assert 'postal_code' in column_names
        assert 'country' in column_names
        assert 'phone' in column_names
        
        assert df.loc[0].to_dict() == {
            "location_id": 2,
            "address_line_1": "179 Alexie Cliffs",
            "address_line_2": None,
            "district": None,
            "city": "Aliso Viejo",
            "postal_code": "99305-7380",
            "country": "San Marino",
            "phone": "9621 880720",
      }
        assert df.loc[1].to_dict() == {
            "location_id": 5,
            "address_line_1": "34177 Upton Track",
            "address_line_2": None,
            "district": None,
            "city": "Fort Shadburgh",
            "postal_code": "55993-8850",
            "country": "Bosnia and Herzegovina",
            "phone": "0081 009772",
      }


    def test_check_dim_design_contains_correct_data(self, s3_mock_with_objects, mock_get_latest_s3_keys, mock_fetch_from_s3, datetime_mock):
        lambda_handler(None, None)

        obj = s3_mock_with_objects.get_object(Bucket='processed-bucket-neural-normalisers',
            Key='processed_data/dim_design/mock_timestamp.parquet')
        
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        assert df.loc[0].to_dict() == {"design_id": 8,  "design_name": "Wooden", "file_location": "/usr", "file_name": "wooden-20220717-npgz.json"}
        assert df.loc[1].to_dict() == {
        "design_id": 51,
        "design_name": "Bronze",
        "file_location": "/private",
        "file_name": "bronze-20221024-4dds.json"}

    def test_check_dim_counterparty_contains_correct_data(self, s3_mock_with_objects, mock_get_latest_s3_keys, mock_fetch_from_s3, datetime_mock):
        lambda_handler(None, None)

        obj = s3_mock_with_objects.get_object(Bucket='processed-bucket-neural-normalisers',
            Key='processed_data/dim_counterparty/mock_timestamp.parquet')
        
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        assert df.loc[0].to_dict() == {'counterparty_id': 3,
                                    'counterparty_legal_address_line_1': "179 Alexie Cliffs",
                                    'counterparty_legal_address_line_2': None,
                                    'counterparty_legal_city': "Aliso Viejo",
                                    'counterparty_legal_country': "San Marino",
                                    'counterparty_legal_district': None,
                                    'counterparty_legal_name': "Armstrong Inc",
                                    'counterparty_legal_phone_number': "9621 880720",
                                    'counterparty_legal_postal_code': "99305-7380",
                                    }
    
    def test_check_dim_date_contains_correct_data(self, s3_mock_with_objects, mock_get_latest_s3_keys, mock_fetch_from_s3, datetime_mock):
        lambda_handler(None, None)

        obj = s3_mock_with_objects.get_object(Bucket='processed-bucket-neural-normalisers',
            Key='processed_data/dim_date/mock_timestamp.parquet')
        
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))

        column_names = list(df.columns)

        assert "year" in column_names
        assert "month" in column_names
        assert "day" in column_names
        assert "day_of_week" in column_names
        assert "day_name" in column_names
        assert "quarter" in column_names
    
    def test_check_dim_staff_contains_correct_data(self, s3_mock_with_objects, mock_get_latest_s3_keys, mock_fetch_from_s3, datetime_mock):
        lambda_handler(None, None)

        obj = s3_mock_with_objects.get_object(Bucket='processed-bucket-neural-normalisers',
            Key='processed_data/dim_staff/mock_timestamp.parquet')
        
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        assert df.loc[0].to_dict() == {'staff_id': 1,
                           'first_name': "Jeremie",
                           'last_name': "Franey",
                           'department_name': "Purchasing",
                           'location': "Manchester",
                           'email_address': "jeremie.franey@terrifictotes.com"}
        assert df.loc[1].to_dict() == {'staff_id': 2,
                           'first_name': "Deron",
                           'last_name': "Beier",
                           'department_name': "Facilities",
                           'location': "Manchester",
                           'email_address': "deron.beier@terrifictotes.com"}
    
    def test_creates_sales_order_parquet_files(self, s3_mock_with_objects, mock_get_latest_s3_keys, mock_fetch_from_s3, datetime_mock):
        lambda_handler(None, None)

        s3_contents = s3_mock_with_objects.list_objects_v2(
                Bucket='processed-bucket-neural-normalisers'
            )
        assert s3_contents['Contents'][6]['Key'] == 'processed_data/fact_sales_order/mock_timestamp.parquet'
        
    def test_check_fact_sales_contains_correct_data(self, s3_mock_with_objects, mock_get_latest_s3_keys, mock_fetch_from_s3, datetime_mock):
        lambda_handler(None, None)

        obj = s3_mock_with_objects.get_object(Bucket='processed-bucket-neural-normalisers',
            Key='processed_data/fact_sales_order/mock_timestamp.parquet')
        
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        first_row = df.loc[0]
        assert first_row.to_dict() == {'sales_record_id': 1,
                                   'sales_order_id': 11293,
                                   'created_date': pd.Timestamp('2024-11-21 00:00:00'),
                                   'created_time': datetime.time(18, 22, 10, 134000),
                                   'last_updated_date': pd.Timestamp('2024-11-21 00:00:00'),
                                   'last_updated_time': datetime.time(18, 22, 10, 134000),
                                   'sales_staff_id': 13,
                                   'counterparty_id': 14,
                                   'units_sold': 41794,
                                   'unit_price': 2.08,
                                   'currency_id': 2,
                                   'design_id': 325,
                                   'agreed_payment_date': pd.Timestamp('2024-11-26 00:00:00'),
                                   'agreed_delivery_date': pd.Timestamp('2024-11-24 00:00:00'),
                                   'agreed_delivery_location_id': 2}
    
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