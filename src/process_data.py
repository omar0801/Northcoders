import pandas as pd
from datetime import datetime
from io import BytesIO
import boto3
import json
import logging
from pprint import pprint

logger = logging.getLogger()
logger.setLevel(logging.INFO)


tables = [
    "address",
    "design",
    "currency",
    "counterparty",
    "staff",
    "department"
    # "sales_order",
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
    return {'dataframe': dim_currency, 'table_name': 'dim_currency'}

def create_dim_counterparty(counterparty, address):
    address = pd.DataFrame(address)
    counterparty = pd.DataFrame(counterparty)
    address_counterparty = pd.merge(counterparty, address, how='left', left_on='legal_address_id', right_on='address_id')
    dim_counterparty = address_counterparty[['counterparty_id', 'counterparty_legal_name', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']]

    dim_counterparty = dim_counterparty.rename(columns={'address_line_1': 'counterparty_legal_address_line_1', 'address_line_2': 'counterparty_legal_address_line_2', 
                                    'district': 'counterparty_legal_district', 'city': 'counterparty_legal_city', 
                                    'postal_code': 'counterparty_postal_code', 'country': 'counterparty_legal_country', 'phone': 'counterparty_phone_number'})
    return {'dataframe': dim_counterparty, 'table_name': 'dim_counterparty'}

def create_dim_staff(staff_data, department_data):
    dataframe_staff = pd.DataFrame(staff_data)
    dataframe_dept = pd.DataFrame(department_data)

    merged_df = pd.merge(dataframe_staff, dataframe_dept, how="inner", on=["department_id", "department_id"])
    dim_staff = merged_df[['staff_id',
                           'first_name',
                           'last_name',
                           'department_name',
                           'location',
                           'email_address']]
    return {'dataframe': dim_staff, 'table_name': 'dim_staff'}

def create_dim_date(start_date = '2000-01-01', end_date='2000-12-31'):

    try:
        df_start_date = pd.Timestamp(start_date)  
        df_end_date = pd.Timestamp(end_date)
        
        start_str_date = df_start_date.strftime('%Y%m%d')
        start_num_date = pd.to_numeric(start_str_date)
        end_str_date = df_end_date.strftime('%Y%m%d')
        end_num_date = pd.to_numeric(end_str_date)

        if end_num_date < start_num_date :
            return "End date must be greter then Start date"

        df = pd.DataFrame({"Date": pd.date_range(start_date, end_date)})
        df["Year"] = df.Date.dt.year
        df["Month"] = df.Date.dt.month_name()
        df["Day"] = df.Date.dt.day
        df["Day_Of_Week"] = df.Date.dt.day_of_week
        df["Day_Name"] = df.Date.dt.day_name()
        df["Quarter"] = df.Date.dt.quarter
        df.set_index(['Date'], inplace=True)
        return {'dataframe': df, 'table_name': 'dim_date'}
    except:
        return "Incorrect Date"

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

    data = {}

    for table in tables:        
        latest_timestamp = get_latest_s3_keys(ingestion_bucket,s3, table)
        s3_key = f'{table}/{latest_timestamp}.json'

        data[table] = fetch_from_s3(ingestion_bucket, s3_key, s3)

    dataframes = []
    dataframes.append(create_dim_location(data['address']))
    dataframes.append(create_dim_design(data['design']))
    dataframes.append(create_dim_currency(data['currency']))
    dataframes.append(create_dim_counterparty(data['counterparty'], data['address']))
    dataframes.append(create_dim_staff(data['staff'], data['department']))
    dataframes.append(create_dim_date(start_date = '2022-01-01', end_date='2025-12-31'))

    if not check_for_fact_sales_parquet(s3):
        #create facts_sales parquet
        pass
    else:
        change_log_timestamp = get_latest_s3_keys(ingestion_bucket,s3, 'changes_log/sales_order')
        sales_change_log_key = f'changes_log/sales_order/{change_log_timestamp}.json'
        change_log_data = fetch_from_s3(ingestion_bucket, sales_change_log_key, s3)
        if any(change_log_data.values()):
            #call update facts_table function
            pass


    for dataframe in dataframes:
        write_dataframe_to_s3(dataframe, s3)



if __name__ == '__main__':
    lambda_handler(None, None)