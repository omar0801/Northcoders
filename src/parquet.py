# from datetime import datetime
# import pandas as pd
# import s3fs
# def write_dataframe_to_s3(df):
    
#     str_timestamp = datetime.now().isoformat()
#     s3 = s3fs.S3FileSystem()
#     df.to_parquet(f"s3://ingestion-bucket-neural-normalisers-new/processed_data/dim_location/{str_timestamp}.parquet", engine='auto', Compression=None)

from datetime import datetime
import pandas as pd
import boto3
from io import BytesIO
#create a list of dim tables to be iterated through as an argument 


def write_dataframe_to_s3(df_dict):
    str_timestamp = datetime.now().isoformat()
    bucket_name = "processing-bucket-neural-normalisers-new"
    key = f"processed_data/{df_dict['table_name']}/{str_timestamp}.parquet"
    
    # Convert DataFrame to Parquet
    parquet_buffer = BytesIO()
    df_dict['dataframe'].to_parquet(parquet_buffer, index=False, engine="pyarrow")

    # Upload to S3
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=parquet_buffer.getvalue()
    )




    # for table in tables:
    #     data['db'][table] = fetch_data_from_table(conn, table)
        
    #     latest_timestamp = get_latest_s3_keys(ingestion_bucket,s3, table)
    #     s3_key = f'{table}/{latest_timestamp}.json'
    #     data['s3'][table] = fetch_from_s3(ingestion_bucket, s3_key, s3)