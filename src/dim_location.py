import pandas as pd
from datetime import datetime
import io

def create_dim_location(address_data):
    address_df = pd.DataFrame(address_data)
    
    location_df = address_df.drop(['created_at', 'last_updated'], axis=1)
    location_df = location_df.rename(columns={'address_id': 'location_id'})

    return {'dataframe': location_df, 'table_name': 'dim_location'}


# def write_dataframe_to_s3(df):
    
#     str_timestamp = datetime.now().isoformat()

#     df.to_parquet(f"s3://ingestion-bucket-neural-normalisers-new/processed_data/dim_location/{str_timestamp}.parquet", engine='auto', Compression=None)






