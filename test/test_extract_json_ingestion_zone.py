from moto import mock_aws
import json, pytest, boto3, logging
from src.extract_json_ingestion_zone import lambda_handler

@pytest.fixture()
def s3_mock():
    with mock_aws():
        s3 = boto3.client('s3', region_name='eu-west-2')
        
        s3.create_bucket(Bucket='test-bucket',
                        CreateBucketConfiguration={
                        'LocationConstraint': 'eu-west-2'}
                        )
        yield s3

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
    
class TestExtractLambdaHandler:
    def test_function_success(self, s3_mock, caplog):
        content = {'test': 'test_value'}
        s3_mock.put_object(
        Bucket='test-bucket',
        Key='test.json',
        Body=json.dumps(content)
        )
        event = generate_s3_event('test-bucket', 'test.json')
        with caplog.at_level(logging.INFO):
            response = lambda_handler(event, None)
        assert response == content
        assert "Lambda handler invoked with event" in caplog.text
        assert "Fetching object from S3" in caplog.text
        assert "Successfully retrieved and parsed object from S3" in caplog.text
        
    def test_object_not_found(self, s3_mock, caplog):
        event = generate_s3_event('test-bucket', 'non-existent.json')
        with caplog.at_level(logging.INFO):
            response = lambda_handler(event, None)
        assert "Object not found in S3" in caplog.text
        
    def test_invalid_json_format(self, s3_mock, caplog):
        s3_mock.put_object(
        Bucket='test-bucket',
        Key='test.json',
        Body='Not a json'
        )
        event = generate_s3_event('test-bucket', 'test.json')
        with caplog.at_level(logging.INFO):
            response = lambda_handler(event, None)
        assert "Failed to decode JSON" in caplog.text
        