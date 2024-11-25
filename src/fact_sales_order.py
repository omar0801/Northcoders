import pandas as pd
# 'data/sales_order.json'

def create_fact_sales_order_table(sales_data):
    sales_order_df = pd.DataFrame(sales_data)
    sales_order_df['created_date'] = pd.to_datetime(sales_order_df['created_at']).dt.date
    sales_order_df['created_date'] = pd.to_datetime(sales_order_df['created_date'], format='%y%m%d')
    sales_order_df['created_time'] = pd.to_datetime(sales_order_df['created_at']).dt.time
    sales_order_df['sales_record_id'] = range(1, len(sales_order_df) + 1)
    date_list = sales_order_df['last_updated'].tolist()
    new_date_list = [pd.to_datetime(date.replace('T', ' ')) for date in date_list]
    sales_order_df['last_update'] = new_date_list
    sales_order_df['last_updated_date'] = pd.to_datetime(sales_order_df['last_update']).dt.date
    sales_order_df['last_updated_date'] = pd.to_datetime(sales_order_df['last_updated_date'], format='%y%m%d')
    sales_order_df['last_updated_time'] = pd.to_datetime(sales_order_df['last_update']).dt.time
    sales_order_df['agreed_payment_date'] = pd.to_datetime(sales_order_df['agreed_payment_date']).dt.date
    sales_order_df['agreed_payment_date'] = pd.to_datetime(sales_order_df['agreed_payment_date'], format='%y%m%d')
    sales_order_df['agreed_delivery_date'] = pd.to_datetime(sales_order_df['agreed_delivery_date']).dt.date
    sales_order_df['agreed_delivery_date'] = pd.to_datetime(sales_order_df['agreed_delivery_date'], format='%y%m%d')
    sales_order_df['sales_staff_id'] = sales_order_df['staff_id']
    fact_sales_order = sales_order_df[['sales_record_id',
                                          'sales_order_id', 'created_date',
                                          'created_time', 'last_updated_date',
                                          'last_updated_time', 'sales_staff_id',
                                          'counterparty_id', 'units_sold', 'unit_price',
                                          'currency_id', 'design_id', 'agreed_payment_date',
                                          'agreed_delivery_date',
                                          'agreed_delivery_location_id']]
    return fact_sales_order

# result = create_fact_sales_order_table('data/sales_order.json')
# print(result)
    