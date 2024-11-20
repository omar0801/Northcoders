import pandas as pd
import json

# with open("/Users/U_Temp/Desktop/NorthCoders/DE NORTHCODERS PROJECT/de-project-specification/data/currency.json", 'r') as file:
# currency_data = json.load(file)



def create_dim_currency(currency_data):
    df = pd.DataFrame(currency_data)
    dim_currency = df[['currency_id', 'currency_code']]
    dim_currency['currency_name'] = ["Great British Pounds", "US Dollars", 'Euros']
    print(dim_currency)
    return {'dataframe': dim_currency, 'table_name': 'dim_currency'}

# create_dim_currency(currency_data)


