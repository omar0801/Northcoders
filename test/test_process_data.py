from src.process_data import *
import pytest
from unittest.mock import patch
from moto import mock_aws



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
        # yield latest_timestamp

combined_data = [[
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
      }], [{"design_id": 8,  "design_name": "Wooden", "file_location": "/usr", "file_name": "wooden-20220717-npgz.json", "created_at": "2022-11-03T14:20:49.962000",
            "last_updated": "2022-11-03T14:20:49.962000"}], [
        {"currency_id": 1, "currency_code": "GBP", "created_at": "2022-11-03T14:20:49", "amount": 100.0},
        {"currency_id": 2, "currency_code": "USD", "created_at": "2022-11-03T14:20:49", "amount": 200.0},
        {"currency_id": 3, "currency_code": "EUR", "created_at": "2022-11-03T14:20:49", "amount": 200.0}
    ]]

# @pytest.fixture()
# def mock_fetch_from_s3():
#     with patch('src.process_data.fetch_from_s3') as data:
#         data.return_value = returned_data()
#         yield data

# def returned_data():
#     for i in range(2):
#         print(i)
#         print(combined_data[i])
#         yield combined_data[i]
        

@pytest.fixture()
def mock_fetch_from_s3():
    with patch('src.process_data.fetch_from_s3') as mock_fetch:
        mock_fetch.side_effect = combined_data
        yield mock_fetch


# @pytest.fixture()
# def mock_create_dim_location():
#     with patch('src.process_data.create_dim_location') as location_df:
#         address_df = pd.DataFrame(address_data)
#         location_df.return_value = {'dataframe': address_df, 'table_name': 'dim_location'}
#         # yield location_df


class TestLambdaHandler():
    def test_creates_dim_location_parquet_file(self, s3_mock_with_objects, mock_get_latest_s3_keys, mock_fetch_from_s3, datetime_mock):
        lambda_handler(None, None)

        s3_contents = s3_mock_with_objects.list_objects_v2(
                Bucket='processed-bucket-neural-normalisers'
            )
        assert s3_contents['Contents'][0]['Key'] == 'processed_data/dim_currency/mock_timestamp.parquet'
        assert s3_contents['Contents'][1]['Key'] == 'processed_data/dim_design/mock_timestamp.parquet'
        assert s3_contents['Contents'][2]['Key'] == 'processed_data/dim_location/mock_timestamp.parquet'
    

    # def test_creates_dim_design_parquet_file(self, s3_mock_with_objects, mock_get_latest_s3_keys, mock_fetch_from_s3, datetime_mock):
    #     lambda_handler(None, None)

    #     s3_contents = s3_mock_with_objects.list_objects_v2(
    #             Bucket='processed-bucket-neural-normalisers'
    #         )
    #     assert s3_contents['Contents'][0]['Key'] == 'processed_data/dim_design/mock_timestamp.parquet'

        
