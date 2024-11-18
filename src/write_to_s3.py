import json
from decimal import Decimal
from datetime import datetime
import os
import boto3
import pg8000.native

tables = [
    "counterparty",
    "currency",
    "department",
    "design",
    "staff",
    "sales_order",
    "address",
    "payment",
    "purchase_order",
    "payment_type",
    "transaction"
]

def connect_to_db():
    return pg8000.native.Connection(
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        database=os.getenv("PG_DATABASE"),
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT"))
    )

def close_db_connection(conn):
    conn.close()

def create_s3_bucket(bucket_prefix, client):

    str_timestamp = datetime.now().isoformat()
    bucket_name = bucket_prefix + str_timestamp

    #format bucket name
    bucket_name = bucket_name.lower()
    bucket_name = bucket_name.replace(':', '')
    bucket_name = bucket_name.replace('.', '')

    try:
        client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration = {
                    'LocationConstraint': 'eu-west-2'
            }
            )
        return bucket_name
    except Exception:
        return 'An error has occured' # To be replaced with logger.log['CRITICAL']
        exit()

def fetch_data_from_table(conn, table_name):
    query = f"SELECT * FROM {table_name};"
    
    result = conn.run(query)
    columns = [col["name"] for col in conn.columns]
    
    data = []
    for row in result:
        row_dict = {}
        for i, value in enumerate(row):
            if isinstance(value, datetime):
                row_dict[columns[i]] = value.isoformat()
            elif isinstance(value, Decimal):
                row_dict[columns[i]] = float(value)
            else:
                row_dict[columns[i]] = value
        data.append(row_dict)
    
    return data

def save_to_json(data, filename):
    filepath = os.path.join("data", filename) 
    os.makedirs("data", exist_ok=True)
    with open(filepath, "w") as f:
        json.dump(data, f, indent=4)
    return f"File '{filename}' has been saved successfully in the 'data' directory."

def save_to_s3(data, bucket_name, filename, client):
    data_JSON = json.dumps(data)
    client.put_object(
        Bucket=bucket_name,
        Body=data_JSON,
        Key=filename
    )

def main(event, context, fetch_func=fetch_data_from_table, save_func=save_to_s3):
    conn = connect_to_db()
    messages = []
    str_timestamp = datetime.now().isoformat()
    client = boto3.client('s3')
    try:
        for table in tables:
            messages.append(f"Extracting data from {table}...")
            data = fetch_func(conn, table)
            json_filename = f"{str_timestamp}/{table}.json"
            
            save_func(data, 'ingestion-bucket-neural-normalisers-new', json_filename, client)
            messages.append("Mock save successful")


    finally:
        close_db_connection(conn)
        messages.append("Database connection closed.")
    return messages
