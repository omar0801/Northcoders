	
import pg8000.native, json, pandas as pd, io, boto3
from sqlalchemy import create_engine
from pprint import pprint
from io import BytesIO

user='project_team_11',
password='Lr4jqizK8rUHkrp',
database='postgres',
host='nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com',
port=5432

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

    s3 = boto3.client('s3')
    bucket = 'processed-bucket-neural-normalisers'

    for table in tables:
        timestamp = get_latest_s3_keys(bucket, s3, table)
        key = f'processed_data/{table}/{timestamp}.parquet'
        print(key)
        parquet_df = fetch_from_s3(bucket, key, s3)

        database_df = pd.read_sql(f'SELECT * FROM {table};', con=engine)

        parquet_df.loc[2] = {'counterparty_id': 32,
                                    'counterparty_legal_address_line_1': "Northcoders Avenue",
                                    'counterparty_legal_address_line_2': None,
                                    'counterparty_legal_city': "Aliso Viejo",
                                    'counterparty_legal_country': "San Marino",
                                    'counterparty_legal_district': None,
                                    'counterparty_legal_name': "Armstrong Inc",
                                    'counterparty_legal_phone_number': "9621 880720",
                                    'counterparty_legal_postal_code': "99305-7380",
                                    }

        #check for modifications

        to_upload_df = pd.DataFrame()
         

        if len(parquet_df) > len(database_df):
            #find additions
            parquet_df.isin(database_df)
            pass
        else:
            #find modifications
            comparision_output = database_df.compare(parquet_df, keep_equal=True)
        



        print(parquet_df)
        print(database_df)

        print(to_upload_df)

        # include_index = False
        # if table == 'dim_date':
        #     include_index = True
        try:
            to_upload_df.to_sql(table, engine, if_exists='append', index=False)
        except:
            print('sql error encountered')

lambda_handler(None, None)

