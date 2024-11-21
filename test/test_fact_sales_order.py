from src.fact_sales_order import create_fact_sales_order_table
import pandas as pd, json, pytest
import datetime


def test_fact_sales_order_returns_pd_dataframe():
    test_output_df = create_fact_sales_order_table('data/sales_order.json')
    assert type(test_output_df) == pd.DataFrame
    
def test_design_df_has_correct_columns():
    test_output_df = create_fact_sales_order_table('data/sales_order.json')
    assert list(test_output_df.columns.values) == ['sales_record_id',
                                          'sales_order_id', 'created_date',
                                          'created_time', 'last_updated_date',
                                          'last_updated_time', 'sales_staff_id',
                                          'counterparty_id', 'units_sold', 'unit_price',
                                          'currency_id', 'design_id', 'agreed_payment_date',
                                          'agreed_delivery_date',
                                          'agreed_delivery_location_id']

def test_design_df_has_correct_data():
    test_output_df = create_fact_sales_order_table('data/sales_order.json')
    first_row = test_output_df.iloc[0]
    assert first_row.to_dict() == {'sales_record_id': 1,
                                   'sales_order_id': 2,
                                   'created_date': pd.Timestamp('2022-11-03 00:00:00'),
                                   'created_time': datetime.time(14, 20, 52, 186000),
                                   'last_updated_date': pd.Timestamp('2022-11-03 00:00:00'),
                                   'last_updated_time': datetime.time(14, 20, 52, 186000),
                                   'sales_staff_id': 19,
                                   'counterparty_id': 8,
                                   'units_sold': 42972,
                                   'unit_price': 3.94,
                                   'currency_id': 2,
                                   'design_id': 3,
                                   'agreed_payment_date': pd.Timestamp('2022-11-08 00:00:00'),
                                   'agreed_delivery_date': pd.Timestamp('2022-11-07 00:00:00'),
                                   'agreed_delivery_location_id': 8}