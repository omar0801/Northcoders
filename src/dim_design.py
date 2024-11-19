import pandas as pd
from pprint import pprint

design = pd.read_json('/home/michael/Northcoders/de-project-specification/data/2024-11-18T13_37_12.390620.json')
print(type(design))
print(design)

def create_dim_design(design_JSON):
    design_df = pd.read_json(design_JSON)
    dim_design = design_df.drop(['created_at', 'last_updated'], axis=1)

    pprint(dim_design)
    return dim_design



# design = pd.read_json('/home/michael/Northcoders/de-project-specification/data/2024-11-18T13_37_12.390620.json')

# dim_design = pd.DataFrame()
# dim_design['design_id'] = design['design_id']
# dim_design['design_name'] = design['design_name']
# dim_design['file_location'] = design['file_location']
# dim_design['file_name'] = design['file_name']

# facts_sales_order = pd.DataFrame()
# facts_sales_order['design_id'] = design['design_id']

# dim_dates = pd.DataFrame()

# dim_dates['created_date'] = design['created_at']
# dim_dates['updated_date'] = design['last_updated']

# pprint(dim_design)
# pprint(facts_sales_order)
# pprint(dim_dates)