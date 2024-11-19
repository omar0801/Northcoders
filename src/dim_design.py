import pandas as pd
from pprint import pprint

# design = pd.read_json('/home/michael/Northcoders/de-project-specification/data/2024-11-18T13_37_12.390620.json')
# print(type(design))
# print(design)

def create_dim_design(design_JSON):
    design_df = pd.json_normalize(design_JSON)
    dim_design = design_df.drop(['created_at', 'last_updated'], axis=1)

    return dim_design
