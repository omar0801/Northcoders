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
    "sales_order",
    # "payment",
    # "purchase_order",
    # "payment_type",
    # "transaction"
]

def get_latest_s3_keys(bucket,s3_client, table_name):
    all_objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=table_name)
    table_name += '/'
    all_key_timestamps = [item['Key'][-31:-5] for item in all_objects['Contents']]
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

def create_fact_sales_order_table(sales_data):
    sales_order_df = pd.DataFrame(sales_data)
    sales_order_df['created_date'] = pd.to_datetime(sales_order_df['created_at']).dt.date
    sales_order_df['created_date'] = pd.to_datetime(sales_order_df['created_date'], format='%y%m%d')
    sales_order_df['created_time'] = pd.to_datetime(sales_order_df['created_at']).dt.time
    sales_order_df['sales_record_id'] = range(1, len(sales_order_df) + 1)
    date_list = sales_order_df['last_updated'].tolist()
    new_date_list = [pd.to_datetime(date.replace('T', ' ')) for date in date_list]
    sales_order_df['last_update'] = new_date_list
    sales_order_df['last_updated_date'] = pd.to_datetime(sales_order_df['last_update']).dt.date
    sales_order_df['last_updated_date'] = pd.to_datetime(sales_order_df['last_updated_date'], format='%y%m%d')
    sales_order_df['last_updated_time'] = pd.to_datetime(sales_order_df['last_update']).dt.time
    sales_order_df['agreed_payment_date'] = pd.to_datetime(sales_order_df['agreed_payment_date']).dt.date
    sales_order_df['agreed_payment_date'] = pd.to_datetime(sales_order_df['agreed_payment_date'], format='%y%m%d')
    sales_order_df['agreed_delivery_date'] = pd.to_datetime(sales_order_df['agreed_delivery_date']).dt.date
    sales_order_df['agreed_delivery_date'] = pd.to_datetime(sales_order_df['agreed_delivery_date'], format='%y%m%d')
    sales_order_df['sales_staff_id'] = sales_order_df['staff_id']
    fact_sales_order = sales_order_df[['sales_record_id',
                                          'sales_order_id', 'created_date',
                                          'created_time', 'last_updated_date',
                                          'last_updated_time', 'sales_staff_id',
                                          'counterparty_id', 'units_sold', 'unit_price',
                                          'currency_id', 'design_id', 'agreed_payment_date',
                                          'agreed_delivery_date',
                                          'agreed_delivery_location_id']]
    fact_sales_order = fact_sales_order.assign(status='current') 
    return {'dataframe': fact_sales_order, 'table_name': 'facts_sales'}

def write_dataframe_to_s3(df_dict, s3):
    str_timestamp = datetime.now().isoformat()
    bucket_name = "processed-bucket-neural-normalisers"
    key = f"processed_data/{df_dict['table_name']}/{str_timestamp}.parquet"
    
    
    parquet_buffer = BytesIO()
    df_dict['dataframe'].to_parquet(parquet_buffer, index=False, engine="pyarrow")

    
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=parquet_buffer.getvalue()
    )

def check_for_fact_sales_parquet(s3_client):
    fact_sales_parquet = s3_client.list_objects_v2(
        Bucket="processed-bucket-neural-normalisers",
        Prefix="processed_data/facts_sales/2"
    )
    return fact_sales_parquet["KeyCount"]


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    ingestion_bucket = "ingestion-bucket-neural-normalisers-new"
    processed_bucket = "processed-bucket-neural-normalisers"
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
   

    if not check_for_fact_sales_parquet(s3):
        sales_df = create_fact_sales_order_table(data['sales_order'])
        write_dataframe_to_s3(sales_df, s3)
    else:
        change_log_timestamp = get_latest_s3_keys(ingestion_bucket,s3, 'changes_log/sales_order')
        sales_change_log_key = f'changes_log/sales_order/{change_log_timestamp}.json'
        change_log_data = fetch_from_s3(ingestion_bucket, sales_change_log_key, s3)
        if any(change_log_data.values()):
            added_records = change_log_data['sales_order']['additions']
            if added_records:
                added_df = create_fact_sales_order_table(added_records)['dataframe']
            deleted_records = change_log_data['sales_order']['deletions']
            if deleted_records:
                sales_order_timestamp = get_latest_s3_keys(processed_bucket, s3, 'processed_data/facts_sales')
                obj = s3.get_object(Bucket=processed_bucket,
                Key= f'processed_data/facts_sales/{sales_order_timestamp}.parquet')
            
                df = pd.read_parquet(BytesIO(obj['Body'].read()))
                for record in deleted_records:
                    df.loc[df['sales_order_id']== record, 'status'] = 'deleted'
            
            sales_order_timestamp = get_latest_s3_keys(processed_bucket, s3, 'processed_data/facts_sales')
            obj = s3.get_object(Bucket=processed_bucket,
            Key= f'processed_data/facts_sales/{sales_order_timestamp}.parquet')
            
            df = pd.read_parquet(BytesIO(obj['Body'].read()))
            frames = [df, added_df]
            output = pd.concat(frames)
            
        
    for dataframe in dataframes:
        write_dataframe_to_s3(dataframe, s3)



if __name__ == '__main__':
    lambda_handler(None, None)