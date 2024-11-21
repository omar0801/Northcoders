import pandas as pd
from datetime import datetime
from io import BytesIO
import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

tables = [
    # "counterparty",
    "address",
    "design",
    "currency",
    # "department",
    # "staff",
    # "sales_order",
    # "payment",
    # "purchase_order",
    # "payment_type",
    # "transaction"
]

def get_latest_s3_keys(bucket,s3_client, table_name):
    all_objects = s3_client.list_objects_v2(Bucket=bucket)
    table_name += '/'
    all_key_timestamps = [item['Key'][-31:-5] for item in all_objects['Contents'] if 'changes_log' not in item['Key'] and table_name in item['Key']]
    latest_timestamp = sorted(all_key_timestamps, reverse=True)[0]
    return latest_timestamp

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


def create_dim_location(address_data):
    address_df = pd.DataFrame(address_data)
    
    location_df = address_df.drop(['created_at', 'last_updated'], axis=1)
    location_df = location_df.rename(columns={'address_id': 'location_id'})

    return {'dataframe': location_df, 'table_name': 'dim_location'}

def create_dim_design(design_JSON):
    design_df = pd.DataFrame(design_JSON)
    dim_design = design_df.drop(['created_at', 'last_updated'], axis=1)

    return {'dataframe': dim_design, 'table_name': 'dim_design'}


def create_dim_currency(currency_data):
    df = pd.DataFrame(currency_data)
    dim_currency = df[['currency_id', 'currency_code']]
    dim_currency['currency_name'] = ["Great British Pounds", "US Dollars", 'Euros']
    print(dim_currency)
    return {'dataframe': dim_currency, 'table_name': 'dim_currency'}

def write_dataframe_to_s3(df_dict, s3):
    str_timestamp = datetime.now().isoformat()
    bucket_name = "processed-bucket-neural-normalisers"
    key = f"processed_data/{df_dict['table_name']}/{str_timestamp}.parquet"
    
    # Convert DataFrame to Parquet
    parquet_buffer = BytesIO()
    df_dict['dataframe'].to_parquet(parquet_buffer, index=False, engine="pyarrow")

    # Upload to S3
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=parquet_buffer.getvalue()
    )




def lambda_handler(event, context):
    s3 = boto3.client('s3')
    ingestion_bucket = "ingestion-bucket-neural-normalisers-new"

    data = {}

    for table in tables:        
        latest_timestamp = get_latest_s3_keys(ingestion_bucket,s3, table)
        s3_key = f'{table}/{latest_timestamp}.json'

        data[table] = fetch_from_s3(ingestion_bucket, s3_key, s3)

    print(data)
    dataframes = []
    dataframes.append(create_dim_location(data['address']))
    dataframes.append(create_dim_design(data['design']))
    dataframes.append(create_dim_currency(data['currency']))

    for dataframe in dataframes:
        write_dataframe_to_s3(dataframe, s3)



if __name__ == '__main__':
    lambda_handler(None, None)



# Updating records from change_log
#