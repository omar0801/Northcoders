import boto3, json, logging, pg8000.native, os, pprint
from datetime import datetime
from decimal import Decimal


logger = logging.getLogger()
logger.setLevel(logging.INFO)



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

def connect_to_db():
    return pg8000.native.Connection(
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        database=os.getenv("PG_DATABASE"),
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT"))
    )

def close_db_connection(conn):
    conn.close()

def fetch_from_s3(bucket, key,s3 ):
    logger.info('Lambda handler invoked with event: %s', json.dumps([bucket, key]))
    try:
        logger.info("Fetching object from S3: bucket=%s, key=%s", bucket, key)
        response = s3.get_object(Bucket=bucket, Key=key)
        content = json.loads(response['Body'].read().decode('utf-8'))
        logger.info("Successfully retrieved and parsed object from S3")
        return content
    
    except s3.exceptions.NoSuchKey:
        logger.error("Object not found in S3: bucket=%s, key=%s", bucket, key)
    except json.JSONDecodeError as e:
        logger.error("Failed to decode JSON: %s", e)

def fetch_data_from_table(conn, table_name):
    query = f"SELECT * FROM {table_name};"
    
    result = conn.run(query)
    columns = [col["name"] for col in conn.columns]
    
    data = []
    for row in result:
        row_dict = {}
        for i, value in enumerate(row):
            if isinstance(value, datetime):
                row_dict[columns[i]] = value.isoformat()
            elif isinstance(value, Decimal):
                row_dict[columns[i]] = float(value)
            else:
                row_dict[columns[i]] = value
        data.append(row_dict)
    
    return data

def get_latest_s3_keys(bucket,s3_client):
    all_objects = s3_client.list_objects_v2(Bucket=bucket)
    all_key_timestamps = [item['Key'][0:26] for item in all_objects['Contents']]
    latest_timestamp = sorted(all_key_timestamps, reverse=True)[0]
    return latest_timestamp


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    out_dict = {'db': {}, 's3': {}}
    ingestion_bucket = "ingestion-bucket-neural-normalisers-new"

    latest_timestamp = get_latest_s3_keys(ingestion_bucket,s3)

    conn = connect_to_db()
    for table in tables:
        out_dict['db'][table] = fetch_data_from_table(conn, table)
        
        s3_key = f'{latest_timestamp}/{table}.json'
        out_dict['s3'][table] = fetch_from_s3(ingestion_bucket, s3_key, s3)

    return out_dict

s3 = boto3.client('s3')
latest_timestamp = get_latest_s3_keys("ingestion-bucket-neural-normalisers-new", s3)
s3_key = '2024-11-18T14:29:05/counterparty.json'
content = fetch_from_s3("ingestion-bucket-neural-normalisers-new", s3_key,s3)
print(content)