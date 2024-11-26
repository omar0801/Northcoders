from src.process_data import create_fact_sales_order_table
import pandas as pd, json, pytest
import datetime

input_data = [{"sales_order_id": 11293, "created_at": "2024-11-21T18:22:10.134000", "last_updated": "2024-11-21T18:22:10.134000",
               "design_id": 325, "staff_id": 13, "counterparty_id": 14, "units_sold": 41794, "unit_price": 2.08, "currency_id": 2, "agreed_delivery_date": "2024-11-24", "agreed_payment_date": "2024-11-26", "agreed_delivery_location_id": 2}, {"sales_order_id": 11294, "created_at": "2024-11-21T18:27:10.106000", "last_updated": "2024-11-21T18:27:10.106000", "design_id": 124, "staff_id": 1, "counterparty_id": 9, "units_sold": 87640, "unit_price": 3.41, "currency_id": 2, "agreed_delivery_date": "2024-11-27", "agreed_payment_date": "2024-11-27", "agreed_delivery_location_id": 28}]


def test_fact_sales_order_returns_pd_dataframe():
    test_output_df = create_fact_sales_order_table(input_data)
    assert type(test_output_df['dataframe']) == pd.DataFrame
    

def test_design_df_has_correct_columns():
    test_output_df = create_fact_sales_order_table(input_data)
    assert list(test_output_df['dataframe'].columns.values) == ['sales_record_id',
                                          'sales_order_id', 'created_date',
                                          'created_time', 'last_updated_date',
                                          'last_updated_time', 'sales_staff_id',
                                          'counterparty_id', 'units_sold', 'unit_price',
                                          'currency_id', 'design_id', 'agreed_payment_date',
                                          'agreed_delivery_date',
                                          'agreed_delivery_location_id']

def test_design_df_has_correct_data():
    test_output_df = create_fact_sales_order_table(input_data)
    first_row = test_output_df['dataframe'].iloc[0]
    assert first_row.to_dict() == {'sales_record_id': 1,
                                   'sales_order_id': 11293,
                                   'created_date': pd.Timestamp('2024-11-21 00:00:00'),
                                   'created_time': datetime.time(18, 22, 10, 134000),
                                   'last_updated_date': pd.Timestamp('2024-11-21 00:00:00'),
                                   'last_updated_time': datetime.time(18, 22, 10, 134000),
                                   'sales_staff_id': 13,
                                   'counterparty_id': 14,
                                   'units_sold': 41794,
                                   'unit_price': 2.08,
                                   'currency_id': 2,
                                   'design_id': 325,
                                   'agreed_payment_date': pd.Timestamp('2024-11-26 00:00:00'),
                                   'agreed_delivery_date': pd.Timestamp('2024-11-24 00:00:00'),
                                   'agreed_delivery_location_id': 2}