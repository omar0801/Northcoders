from datetime import datetime
import boto3
import json
import logging
import pandas as pd
from io import BytesIO

logger = logging.getLogger()
logger.setLevel(logging.INFO)

tables = [
    'dim_counterparty',
    'dim_currency',
    'dim_date',
    'dim_design',
    'dim_location',
    'dim_staff'
    # "sales_order"
    ]

def get_latest_s3_keys(bucket,s3_client, table_name):
    prefix = f"processed_data/{table_name}/"
    all_objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    # table_name += '/'
    all_key_timestamps = [item['Key'][-34:-8] for item in all_objects['Contents']]
    latest_timestamp = sorted(all_key_timestamps, reverse=True)[0]
    return latest_timestamp

def fetch_from_s3(bucket, key,s3 ):
    response = s3.get_object(Bucket=bucket, Key=key)
    content = BytesIO(response['Body'].read())
    df = pd.read_parquet(content)
    return df


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    processed_bucket = "processed-bucket-neural-normalisers"

    data = {}

    for table in tables:        
        latest_timestamp = get_latest_s3_keys(processed_bucket,s3, table)
        s3_key = f'processed_data/{table}/{latest_timestamp}.parquet'

        data[table] = fetch_from_s3(processed_bucket, s3_key, s3)

    for table, df in data.items():
        print(f"Table: {table}")
        print(df)

if __name__ == '__main__':
    lambda_handler(None, None)
