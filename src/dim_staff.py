import pandas as pd
# 'data/staff.json'
# 'data/department.json'

def create_dim_staff(filepath_1, filepath_2):
    dataframe_staff = pd.read_json (filepath_1, orient='records', encoding='utf-8')
    dataframe_dept = pd.read_json (filepath_2, orient='records', encoding='utf-8')
    merged_df = pd.merge(dataframe_staff, dataframe_dept, how="inner", on=["department_id", "department_id"])
    dim_staff = merged_df[['staff_id', 'first_name', 'last_name', 'department_name', 'location']].set_index('staff_id')
    return dim_staff

result = create_dim_staff('data/staff.json', 'data/department.json')
print(result)