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
    "department",
    "sales_order"
]

def get_latest_s3_keys(bucket,s3_client, table_name):

    """
    Finds the latest timestamped file for a specific table in an S3 bucket.

    Args:
        bucket: The S3 bucket name.
        s3_client: The S3 client to interact with S3.
        table_name: The name of the table to look for.

    Returns:
        str: The latest timestamp of the file.
    """
    
    all_objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=table_name)
    table_name += '/'
    all_key_timestamps = [item['Key'][-31:-5] for item in all_objects['Contents']]
    latest_timestamp = sorted(all_key_timestamps, reverse=True)[0]
    return latest_timestamp

def fetch_from_s3(bucket, key,s3 ):

    """
    Fetches and reads JSON data from S3.

    Args:
        bucket: Name of the S3 bucket.
        key: Key of the object to retrieve.
        s3: S3 client instance.

    Returns:
        dict: Parsed JSON content from the S3 object.
    """

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

    """
    Creates the table 'dim_location' from address data.

    Args:
        address_data (list): List of address records.

    Returns:
        dict: Dataframe and table name for the dim_location table.
    """

    address_df = pd.DataFrame(address_data)
    
    location_df = address_df.drop(['created_at', 'last_updated'], axis=1)
    location_df = location_df.rename(columns={'address_id': 'location_id'})

    return {'dataframe': location_df, 'table_name': 'dim_location'}

def create_dim_design(design_JSON):

    """
    Creates the table 'dim_design' from design data.

    Args:
        design_JSON (list): List of design records.

    Returns:
        dict: Dataframe and table name for the dim_design table.
    """

    design_df = pd.DataFrame(design_JSON)
    dim_design = design_df.drop(['created_at', 'last_updated'], axis=1)

    return {'dataframe': dim_design, 'table_name': 'dim_design'}

def create_dim_currency(currency_data):

    """
    Creates the table 'dim_currency' from currency data.

    Args:
        currency_data (list): List of currency records.

    Returns:
        dict: Dataframe and table name for the dim_currency table.
    """

    df = pd.DataFrame(currency_data)
    dim_currency = df[['currency_id', 'currency_code']]
    dim_currency['currency_name'] = ["Great British Pounds", "US Dollars", 'Euros']
    return {'dataframe': dim_currency, 'table_name': 'dim_currency'}

def create_fact_sales_order_table(sales_data):

    """
    Creates the fact sales order table from sales data.

    Args:
        sales_data (list): List of sales order records.

    Returns:
        dict: Dataframe and table name for the fact sales order table.
    """

    sales_order_df = pd.DataFrame(sales_data)
    sales_order_df['created_date'] = pd.to_datetime(sales_order_df['created_at'], format='ISO8601').dt.date
    sales_order_df['created_date'] = pd.to_datetime(sales_order_df['created_date'], format='%y%m%d')
    sales_order_df['created_time'] = pd.to_datetime(sales_order_df['created_at'], format='ISO8601').dt.time
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
    return {'dataframe': fact_sales_order, 'table_name': 'fact_sales_order'}

def create_dim_counterparty(counterparty, address):

    """
    Creates the table 'dim_counterparty' by merging 'counterparty' and 'address' data.

    Args:
        counterparty (list): List of counterparty records.
        address (list): List of address records.

    Returns:
        dict: Dataframe and table name for the dim_counterparty table.
    """

    address = pd.DataFrame(address)
    counterparty = pd.DataFrame(counterparty)
    address_counterparty = pd.merge(counterparty, address, how='left', left_on='legal_address_id', right_on='address_id')
    dim_counterparty = address_counterparty[['counterparty_id', 'counterparty_legal_name', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']]

    dim_counterparty = dim_counterparty.rename(columns={'address_line_1': 'counterparty_legal_address_line_1', 'address_line_2': 'counterparty_legal_address_line_2', 
                                    'district': 'counterparty_legal_district', 'city': 'counterparty_legal_city', 
                                    'postal_code': 'counterparty_legal_postal_code', 'country': 'counterparty_legal_country', 'phone': 'counterparty_legal_phone_number'})
    return {'dataframe': dim_counterparty, 'table_name': 'dim_counterparty'}

def create_dim_staff(staff_data, department_data):

    """
    Creates the table 'dim_staff' by merging staff and department data.

    Args:
        staff_data (list): List of staff records.
        department_data (list): List of department records.

    Returns:
        dict: Dataframe and table name for the dimensional staff table.
    """

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

def create_dim_date(start_date = '2022-01-01', end_date='2025-12-31'):

    """
    Creates a table 'dim_date' for a given range.

    Args:
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.

    Returns:
        dict: Dataframe and table name for the dim_date table.
    """

    try:
        df_start_date = pd.Timestamp(start_date)  
        df_end_date = pd.Timestamp(end_date)
        
        start_str_date = df_start_date.strftime('%Y%m%d')
        start_num_date = pd.to_numeric(start_str_date)
        end_str_date = df_end_date.strftime('%Y%m%d')
        end_num_date = pd.to_numeric(end_str_date)

        if end_num_date < start_num_date :
            return "End date must be greter then Start date"

        df = pd.DataFrame({"date_id": pd.date_range(start_date, end_date)})
        df["date_id"]
        df["year"] = df.date_id.dt.year
        df["month"] = df.date_id.dt.month
        df["day"] = df.date_id.dt.day
        df["day_of_week"] = df.date_id.dt.day_of_week
        df["day_name"] = df.date_id.dt.day_name()
        df['month_name'] = df.date_id.dt.month_name()
        df["quarter"] = df.date_id.dt.quarter
        df.set_index(['date_id'], inplace=False)
        return {'dataframe': df, 'table_name': 'dim_date'}
    except:
        return "Incorrect Date"

def write_dataframe_to_s3(df_dict, s3):

    """
    Writes a dataframe to S3, stored as a parquet file.

    Args:
        df_dict (dict): Dictionary containing the dataframe and table name.
        s3 (boto3.client): S3 client instance.
    """

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

    """
    Checks if a fact sales parquet file exists in the S3 bucket.

    Args:
        s3_client (boto3.client): S3 client instance.

    Returns:
        int: Number of matching parquet files.
    """

    fact_sales_parquet = s3_client.list_objects_v2(
        Bucket="processed-bucket-neural-normalisers",
        Prefix="processed_data/facts_sales/2"
    )
    return fact_sales_parquet["KeyCount"]


def lambda_handler(event, context):

    """
    Starting point for AWS Lambda function execution. Processes data from S3 to create d'im' and 'fact' tables.

    Args:
        event (dict): Event triggering the Lambda.
        context (object): Context of the Lambda execution.

    Returns:
        None
    """

    s3 = boto3.client('s3')
    ingestion_bucket = "ingestion-bucket-neural-normalisers-new"
    processed_bucket = "processed-bucket-neural-normalisers"
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
    dataframes.append(create_fact_sales_order_table(data['sales_order']))            
            
        
    for dataframe in dataframes:
        write_dataframe_to_s3(dataframe, s3)



if __name__ == '__main__':
    lambda_handler(None, None)