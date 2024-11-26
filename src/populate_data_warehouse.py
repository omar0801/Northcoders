	
import pg8000.native, json, pandas as pd, io, boto3
from sqlalchemy import create_engine
from pprint import pprint
from src.fact_sales_order import create_fact_sales_order_table
from src.process_data import get_latest_s3_keys, fetch_from_s3, create_dim_date
from io import BytesIO
import numpy as np
import datetime

user='project_team_11',
password='Lr4jqizK8rUHkrp',
database='postgres',
host='nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com',
port=5432

tables = [
    # 'dim_date',
    # 'dim_location', 
    # 'dim_design',
    # 'dim_currency',
    # 'dim_counterparty',
    # 'dim_staff', 
    'fact_sales_order'
]

def connect_to_db():
    return pg8000.native.Connection(
        user='project_team_11',
        password='Lr4jqizK8rUHkrp',
        database='postgres',
        host='nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com',
        port=5432
    )

def close_db_connection(conn):
    conn.close()

def get_latest_s3_keys(bucket,s3_client, table_name):
    prefix = f"processed_data/{table_name}/"
    all_objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    all_key_timestamps = [item['Key'][-34:-8] for item in all_objects['Contents']]
    latest_timestamp = sorted(all_key_timestamps, reverse=True)[0]
    return latest_timestamp

def fetch_from_s3(bucket, key,s3 ):
    response = s3.get_object(Bucket=bucket, Key=key)
    content = BytesIO(response['Body'].read())
    df = pd.read_parquet(content)
    return df


def lambda_handler(event, context):

    engine = create_engine('postgresql://project_team_11:Lr4jqizK8rUHkrp@nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com:5432/postgres')
    engine.dispose()

    s3 = boto3.client('s3')
    bucket = 'processed-bucket-neural-normalisers'

    for table in tables:
        timestamp = get_latest_s3_keys(bucket, s3, table)
        key = f'processed_data/{table}/{timestamp}.parquet'
        print(key)
        parquet_df = fetch_from_s3(bucket, key, s3)

        database_df = pd.read_sql(f'SELECT * FROM {table};', con=engine)

        database_df['created_date'] = pd.to_datetime(database_df['created_date'], format='%y%m%d')
        database_df['last_updated_date'] = pd.to_datetime(database_df['last_updated_date'], format='%y%m%d')
        database_df['agreed_payment_date'] = pd.to_datetime(database_df['agreed_payment_date'], format='%y%m%d')
        database_df['agreed_delivery_date'] = pd.to_datetime(database_df['agreed_delivery_date'], format='%y%m%d')

        # print(database_df)
        parquet_df.loc[2, 'design_id'] = 7
        # print(parquet_df)
        # print(parquet_df)
        # parquet_df.loc[len(parquet_df)] = {'sales_record_id': 11361,
        #                            'sales_order_id': 11361,
        #                            'created_date': pd.Timestamp('2024-11-21 00:00:00'),
        #                            'created_time': datetime.time(18, 22, 10, 134000),
        #                            'last_updated_date': pd.Timestamp('2024-11-21 00:00:00'),
        #                            'last_updated_time': datetime.time(18, 22, 10, 134000),
        #                            'sales_staff_id': 13,
        #                            'counterparty_id': 14,
        #                            'units_sold': 41794,
        #                            'unit_price': 2.08,
        #                            'currency_id': 2,
        #                            'design_id': 325,
        #                            'agreed_payment_date': pd.Timestamp('2024-11-26 00:00:00'),
        #                            'agreed_delivery_date': pd.Timestamp('2024-11-24 00:00:00'),
        #                            'agreed_delivery_location_id': 2}
        #check for modifications

        # to_upload_df = pd.DataFrame()
        frames = []
        # # .query('_merge == "left_only"').drop('_merge', axis=1)

        # print(database_df.iloc[5501:5507])
        # print(parquet_df.iloc[5501:5507])


        if len(parquet_df) > len(database_df):
            diff = len(parquet_df) > len(database_df)
            added_records = parquet_df[-diff:]
            frames.append(added_records)
            print('records added')
        # else:
        if table == 'fact_sales_order':
            df_diff = pd.merge(parquet_df, database_df, how='outer', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
            # df_diff = df_diff.drop(columns=['sales_record_id'])
            df_diff['sales_record_id'] = (list(pd.RangeIndex(start=len(database_df)+2, stop=len(database_df)+len(df_diff)+2)))
            
            # print(df_diff, '<-------- difference')
            # print('new line')
            frames.append(df_diff)
            
            print('differences detected')

        if frames:
            to_upload_df = pd.concat(frames)




        # print(parquet_df)
        # print(database_df)

        # print(to_upload_df)

        # include_index = False
        # if table == 'dim_date':
        #     include_index = True
        # df_diff.reset_index(drop=True)
        # print(df_diff)
            to_upload_df.to_sql(table, engine, if_exists='append', index=False)
        # try:
        #     to_upload_df.to_sql(table, engine, if_exists='append', index=False)
        # except:
        #     print('sql error encountered')

lambda_handler(None, None)

