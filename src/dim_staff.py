import pandas as pd
# 'data/staff.json'
# 'data/department.json'
# .set_index('staff_id')

def create_dim_staff(staff_data, department_data):
    dataframe_staff = pd.DataFrame(staff_data)
    dataframe_dept = pd.DataFrame(department_data)

    merged_df = pd.merge(dataframe_staff, dataframe_dept, how="inner", on=["department_id", "department_id"])
    dim_staff = merged_df[['staff_id',
                           'first_name',
                           'last_name',
                           'department_name',
                           'location',
                           'email_address']]
    return {'dataframe': dim_staff, 'table_name': 'dim_staff'}