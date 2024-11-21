from src.process_data import create_dim_date
import pytest
import pandas as pd

def test_on_correct_date_format():
    
    start_date = '2000-01-01'
    end_date='2000-12-31'
    df = create_dim_date(start_date, end_date)
    df_value = df['dataframe'].index.values[0]
    df_to_date = pd.Timestamp(df_value)
    output = df_to_date.strftime('%Y-%m-%d')
    assert output == '2000-01-01'
    
def test_on_in_correct_start_date_format():
    
    start_date = '200001-01'
    end_date='2000-12-31'
    output = create_dim_date(start_date, end_date)
    assert output == "Incorrect Date"
    
def test_on_in_correct_end_date_format():
    
    start_date = '2000-01-01'
    end_date='200012-31'
    output = create_dim_date(start_date, end_date)
    assert output == "Incorrect Date"
    
def test_on_start_date_and_end_date_other_way_round():
    start_date = '2000-01-01'
    end_date='1900-12-31'
    output = create_dim_date(start_date, end_date)
    assert output == "End date must be greter then Start date"