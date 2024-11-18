import boto3, json, logging, pg8000.native, os, pprint
from datetime import datetime
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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

def get_latest_s3_keys(bucket,s3_client):
    all_objects = s3_client.list_objects_v2(Bucket=bucket)
    all_key_timestamps = [item['Key'][-31:-5] for item in all_objects['Contents'] if 'changes_log' not in item['Key']]
    latest_timestamp = sorted(all_key_timestamps, reverse=True)[0]
    return latest_timestamp

def save_to_s3(data, bucket_name, filename, client):
    data_JSON = json.dumps(data)
    client.put_object(
        Bucket=bucket_name,
        Body=data_JSON,
        Key=filename
    )

def check_additions(db_data, s3_data):

    change_log = {}
    if not db_data:
        change_log['message'] = 'No records found in database'
        return change_log
    
    if not s3_data:
        change_log['records'] = 'no s3 data'
        return change_log

    
    last_record = s3_data[-1]
    last_id = list(last_record.values())[0]    
    temp_rec = []
    for rec in reversed(db_data):
        id_value = list(rec.values())[0]
        if id_value > last_id:
            temp_rec.append(rec)
            change_log['message'] = "Addition detected" 
        else:
            break
    
    if not temp_rec:
        change_log['message'] = "No addition found"
    
    temp_rec.reverse()
    if temp_rec:
        change_log['records'] = temp_rec 
    
    return change_log
    
def check_deletions(db_data, s3_data):

    
    delete_log = {}

    if not s3_data:
        delete_log['records'] = 'no s3 data'
        return delete_log

    s3_id_list = [list(rec.values())[0] for rec in s3_data]
    db_id_list = [list(rec.values())[0] for rec in db_data]

    deleted_list = []
    for item in s3_id_list:
        if item not in db_id_list:
            deleted_list.append(item)
            delete_log['message'] = 'Deletion detected'
    
    if deleted_list:
        delete_log['records'] = deleted_list
    else:
        delete_log['message'] = 'No deletions detected'

    return delete_log
    
def check_changes(db_data, s3_data):
    change_log = {}

    if not s3_data:
        change_log['records'] = 'no s3 data'
        return change_log

    s3_dict = {list(rec.values())[0]: rec for rec in s3_data}
    db_dict = {list(rec.values())[0]: rec for rec in db_data}
    keys = list(s3_data[0].keys())

    changed_recs = []
    for id, data in db_dict.items():
        # if id not in list(s3_dict.values()):
        try:
            if s3_dict[id] != data:
                changed_rec = {'id': id}
                for key in keys:
                    if s3_dict[id][key] != data[key]:
                        changed_rec[key] = data[key]
                changed_recs.append(changed_rec)
        except KeyError:
            continue


    if changed_recs:
        change_log['message'] = 'Changes detected'
        change_log['records'] = changed_recs  
    else:
        change_log['message'] = 'No changes detected'

        
    return change_log

def main_check_for_changes(event, client):
    db = event['db']
    s3 = event['s3']

    keys = list(s3.keys())
    str_timestamp = datetime.now().isoformat()
    change_detected = []

    for table in keys:
        changes_rec = {}
        changes_rec[table] = {}

        addition_result = check_additions(db[table], s3[table])
        if 'records' in list(addition_result.keys()):
            changes_rec[table]['additions'] = addition_result['records']
        else:
            changes_rec[table]['additions'] = None
        
        deletions_result = check_deletions(db[table], s3[table])
        if 'records' in list(deletions_result.keys()):
            changes_rec[table]['deletions'] = deletions_result['records']
        else:
            changes_rec[table]['deletions'] = None

        changes_result = check_changes(db[table], s3[table])
        if 'records' in list(changes_result.keys()):
            changes_rec[table]['changes'] = changes_result['records']
        else:
            changes_rec[table]['changes'] = None
        if any(changes_rec[table].values()):
            change_detected.append(table)
        

        
        data_JSON = json.dumps(changes_rec)
        client.put_object(
            Bucket= 'ingestion-bucket-neural-normalisers-new',
            Body=data_JSON,
            Key= f"changes_log/{table}/{str_timestamp}.json"
            )
    
    return change_detected

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    conn = connect_to_db()
    data = {'db': {}, 's3': {}}
    ingestion_bucket = "ingestion-bucket-neural-normalisers-new"

    latest_timestamp = get_latest_s3_keys(ingestion_bucket,s3)


    for table in tables:
        data['db'][table] = fetch_data_from_table(conn, table)
        
        s3_key = f'{table}/{latest_timestamp}.json'
        data['s3'][table] = fetch_from_s3(ingestion_bucket, s3_key, s3)


    changed_tables = main_check_for_changes(data, s3)

    str_timestamp = datetime.now().isoformat()

    for table in tables:
        json_filename = f"{table}/{str_timestamp}.json"
        save_to_s3(data['db'][table], ingestion_bucket, json_filename, s3)



lambda_handler(None, None)
