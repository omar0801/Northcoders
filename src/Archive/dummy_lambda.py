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
    conn = connect_to_db()
    # conn.run("UPDATE facts_sales_order SET units_sold = -1 WHERE day_name = 'Friday';")
    # empty_table = conn.run('DELETE FROM dim_date;')
    empty_table = conn.run('SELECT * FROM fact_sales_order ORDER BY sales_record_id DESC LIMIT 1 ;')
    # columns_names = [col['name'] for col in conn.columns]
    # print(columns_names)

    pprint(empty_table)

    #create dates df
    # date_df = create_dim_date()['dataframe']
    # print(date_df)

    # engine = create_engine('postgresql://project_team_11:Lr4jqizK8rUHkrp@nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com:5432/postgres')

    # date_df.to_sql('new_dim_date', engine, if_exists='append')

    # print('REPLACED DATA!!!!!!!!!!!!!!!!!!!!!!')

    # pty_table = conn.run('SELECT * FROM dim_date;')
    # pprint(empty_table)

    close_db_connection(conn)


lambda_handler()