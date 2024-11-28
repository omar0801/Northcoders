	
import pg8000.native, json, pandas as pd, io, boto3
from sqlalchemy import create_engine
from pprint import pprint
from io import BytesIO
import numpy as np
import os

tables = [
    'dim_date',
    'dim_location', 
    'dim_design',
    'dim_currency',
    'dim_counterparty',
    'dim_staff', 
    'fact_sales_order'
]

def connect_to_db():
    '''Creates and returns a pg8000 database connection object getting the required paramters from enviroment variables.

    Args:
        None

    Returns:
        pg8000 database connection object
    '''
    return pg8000.native.Connection(
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD_DW"),
        database=os.getenv("PG_DATABASE_DW"),
        host=os.getenv("PG_HOST_DW"),
        port=int(os.getenv("PG_PORT"))
    )

def close_db_connection(conn):
    '''
    Closes passed pg8000 database connection
    
    Args:
        pg8000 database connection object

    Returns:
        None
    '''
    conn.close()

def get_latest_s3_keys(bucket,s3_client, table_name):
    """
    Finds the latest two timestamps of Parquet files for a specific table in an S3 bucket.

    Args:
        bucket: The S3 bucket name.
        s3_client: The S3 client to interact with S3.
        table_name: The name of the table to look for.

    Returns:
        list: The latest two Parquet file timestamps.
    """
    prefix = f"processed_data/{table_name}/"
    all_objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    all_key_timestamps = [item['Key'][-34:-8] for item in all_objects['Contents']]
    latest_timestamp = sorted(all_key_timestamps, reverse=True)[0]
    previous_timestamp = sorted(all_key_timestamps, reverse=True)[1]
    return [latest_timestamp, previous_timestamp]

def fetch_from_s3(bucket, key, s3):
    """
    Fetches and reads parquet data from S3.

    Args:
        bucket: Name of the S3 bucket.
        key: Key of the object to retrieve.
        s3: S3 client instance.

    Returns:
        Pandas DataFrame: the parsed Parquet data from the S3 bucket.
    """
    response = s3.get_object(Bucket=bucket, Key=key)
    content = BytesIO(response['Body'].read())
    df = pd.read_parquet(content)
    return df


def lambda_handler(event, context):
    """
    Starting point for AWS Lambda function execution. Populates the data warehouse with data from the S3 bucket for processed data.

    Args:
        event (dict): Event triggering the Lambda.
        context (object): Context of the Lambda execution.

    Returns:
        None
    """
    engine = create_engine(
        url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
            os.getenv("PG_USER"), os.getenv("PG_PASSWORD_DW"), os.getenv("PG_HOST_DW"), int(os.getenv("PG_PORT")), os.getenv("PG_DATABASE_DW")
        ))

    s3 = boto3.client('s3')
    bucket = 'processed-bucket-neural-normalisers'

    for table in tables:
        print(f'processing: {table}')
        conn = connect_to_db()
        has_row = conn.run(f'select exists(select * from {table}) as has_row')[0][0]
        latest_timestamp = get_latest_s3_keys(bucket, s3, table)[0]
        latest_key = f'processed_data/{table}/{latest_timestamp}.parquet'
        latest_df = fetch_from_s3(bucket, latest_key, s3)
        print(latest_key)
        previous_timestamp = get_latest_s3_keys(bucket, s3, table)[1]
        previous_key = f'processed_data/{table}/{previous_timestamp}.parquet'
        previous_df = fetch_from_s3(bucket, previous_key, s3)
        print(previous_key)

        if not has_row:
            print(f'{table} populated')
            latest_df.to_sql(table, engine, if_exists='append', index=False)
        else:
            if table == 'fact_sales_order':

                df_diff = pd.merge(latest_df, previous_df, how='outer', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
                
                last_id = pd.read_sql(f'SELECT sales_record_id FROM {table} ORDER BY sales_record_id DESC LIMIT 1;', con=engine)
                last_id = last_id['sales_record_id'].values[0]

                df_diff['sales_record_id'] = (list(pd.RangeIndex(start=last_id+1, stop=(last_id+1)+len(df_diff))))
              
            else:
                if len(latest_df) > len(previous_df):
                    diff = len(latest_df) > len(previous_df)
                    added_records = latest_df[-diff:]
                    added_records.to_sql(table, engine, if_exists='append', index=False)

