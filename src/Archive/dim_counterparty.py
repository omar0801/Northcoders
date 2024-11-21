import pandas as pd

def create_dim_counterparty(counterparty, address):
    address = pd.DataFrame(address)
    counterparty = pd.DataFrame(counterparty)
    address_counterparty = pd.merge(counterparty, address, how='left', left_on='legal_address_id', right_on='address_id')
    dim_counterparty = address_counterparty[['counterparty_id', 'counterparty_legal_name', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']]

    dim_counterparty = dim_counterparty.rename(columns={'address_line_1': 'counterparty_legal_address_line_1', 'address_line_2': 'counterparty_legal_address_line_2', 
                                    'district': 'counterparty_legal_district', 'city': 'counterparty_legal_city', 
                                    'postal_code': 'counterparty_postal_code', 'country': 'counterparty_legal_country', 'phone': 'counterparty_phone_number'})
    return dim_counterparty


# def lambda_handler(event, context):
#     #data = fetch_from_s3()

#     dim_couterparty = create_dim_counterparty(address, counterparty)

#     #write_to_s3(dim_couterparty)

