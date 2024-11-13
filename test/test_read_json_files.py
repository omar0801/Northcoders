from moto import mock_aws
import json, pytest, boto3
from src.read_json_files import lambda_handler

@pytest.fixture()
def s3_mock():
    with mock_aws():
        s3 = boto3.client('s3', region_name='eu-west-2')
        
        s3.create_bucket(Bucket='test-bucket',
                            CreateBucketConfiguration={
                                'LocationConstraint': 'eu-west-2'}
                         )
        yield s3
        
def test_function_returns_dict(s3_mock):
    result = lambda_handler({}, {})
    assert isinstance(result, dict)
 
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
    
    
def test_function_success(s3_mock):
    s3_mock.put_object(
    Bucket='test-bucket',
    Key='test.json',
    Body=json.dumps({'test': 'test_value'})
    )
    event = generate_s3_event('test-bucket', 'test.json')
    
    response = lambda_handler(event, None)
    assert response['status'] == 'success'
