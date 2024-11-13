import boto3, json, logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
def lambda_handler(event, context):
    logger.info('Lambda handler invoked with event: %s', json.dumps(event))
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    try:
        logger.info("Fetching object from S3: bucket=%s, key=%s", bucket, key)
        response = s3.get_object(Bucket=bucket, Key=key)
        content = json.loads(response['Body'].read().decode('utf-8'))
        logger.info("Successfully retrieved and parsed object from S3")
        return {
                'status': 'success',
                'data': content
            }
    except s3.exceptions.NoSuchKey:
        logger.error("Object not found in S3: bucket=%s, key=%s", bucket, key)
        return {
            "status": "error",
            "message": "Object does not exist"
        }
    except json.JSONDecodeError as e:
        logger.error("Failed to decode JSON: %s", e)
        return {
            "status": "error",
            "message": "Invalid JSON format"
        }