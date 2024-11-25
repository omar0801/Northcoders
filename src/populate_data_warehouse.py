	
import pg8000.native, json, pandas as pd, io, boto3
from sqlalchemy import create_engine
from pprint import pprint
from src.fact_sales_order import create_fact_sales_order_table
from src.process_data import get_latest_s3_keys, fetch_from_s3, create_dim_date

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


def lambda_handler():

    #connect to data wharehoure
    # conn = connect_to_db()
    engine = create_engine('postgresql://project_team_11:Lr4jqizK8rUHkrp@nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com:5432/postgres')

    s3 = boto3.client('s3')

    #retriev table data
    for table in tables:
        #get latest timestamp

        key = f'processed_data/{table}/{timestamp}.parquet'
        processed_data = s3.get_object(Bucket='processed-bucket-neural-normalisers',
                Key=key)
            
        df = pd.read_parquet(io.BytesIO(processed_data['Body'].read()))

        include_index = False
        if table == 'dime_date':
            include_index = True
        
        df.to_sql(table, engine, if_exists='replace', index=include_index)

