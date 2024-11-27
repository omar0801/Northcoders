import awswrangler as wr, boto3, pandas as pd
import datetime
from fact_sales_order import create_fact_sales_order_table

def accessing_facts_table():
    new_record = {'sales_record_id': [3],
        'sales_order_id': [4],
        'created_date': [pd.Timestamp('2022-11-03 00:00:00')],
        'created_time': [datetime.time(14, 50, 52, 186000)],
        'last_updated_date': [pd.Timestamp('2022-11-03 00:00:00')],
        'last_updated_time': [datetime.time(14, 50, 52, 186000)],
        'sales_staff_id': [10],
        'counterparty_id': [16],
        'units_sold': [32100],
        'unit_price': [3.95],
        'currency_id': [2],
        'design_id': [4],
        'agreed_payment_date': [pd.Timestamp('2022-11-07 00:00:00')],
        'agreed_delivery_date': [pd.Timestamp('2022-11-05 00:00:00')],
        'agreed_delivery_location_id': [15]}





    df = create_fact_sales_order_table('data/sales_order.json')
    bucket = 'aws-wrangler-facts-table-bucket'
    path1 = f"s3://{bucket}/dataset/"
    path2 = f"s3://{bucket}/dataset1/"

    rec_df = pd.DataFrame(new_record)
    wr.s3.to_parquet(df=df, path=path1, dataset=True, mode="overwrite")
    wr.s3.read_parquet(path1, dataset=True)

    wr.s3.to_parquet(df=rec_df, path=path2, dataset=True, mode="overwrite")
    wr.s3.read_parquet(path2, dataset=True)



    wr.s3.merge_datasets(source_path=path2, target_path=path1, mode="append")
    file = wr.s3.read_parquet(path1, dataset=True)
    return file


def check_additions():
    sales_df = create_fact_sales_order_table('data/sales_order.json')
    fact_df = accessing_facts_table()
    
    change_log = {}
    # if not db_data:
    #     change_log['message'] = 'No records found in database'
    #     return change_log
    
    last_record = sales_df.iloc[-1]
    print(last_record)
    fact_last_rec = fact_df.iloc[-1]
    print(fact_last_rec)
    # last_id = list(last_record.values())[0]    
    # print(last_id)
    temp_rec = []
    # for rec in reversed(db_data):
    #     id_value = list(rec.values())[0]
    #     if id_value > last_id:
    #         temp_rec.append(rec)
    #         change_log['message'] = "Addition detected" 
    #     else:
    #         break
    
    # if not temp_rec:
    #     change_log['message'] = "No addition found"
    
    # temp_rec.reverse()
    # if temp_rec:
    #     change_log['records'] = temp_rec 
    
    # return change_log
    
check_additions()


