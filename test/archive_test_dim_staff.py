# from src.dim_staff import create_dim_staff
# import pandas as pd, json, pytest

# with open('data/staff.json', 'r') as staff_json_file:
#     staff_json = json.load(staff_json_file)
    
# with open('data/department.json', 'r') as dept_json_file:
#     dept_json = json.load(dept_json_file)
    
# def test_dim_staff_table_returns_pd_dataframe():
#     response = create_dim_staff('data/staff.json', 'data/department.json')
#     assert type(response) == pd.DataFrame
    
# def test_dim_staff_df_has_correct_columns():
#     response = create_dim_staff('data/staff.json', 'data/department.json')
#     assert list(response.columns.values) == ['staff_id',
#                                            'first_name',
#                                            'last_name',
#                                            'department_name',
#                                            'location',
#                                            'email_address']
# def test_design_df_has_correct_data():
#     response = create_dim_staff('data/staff.json', 'data/department.json')
#     first_row = response.iloc[0]
#     assert first_row.to_dict() == {'staff_id': 1, 
#                                    'first_name': 'Jeremie',
#                                    "last_name": 'Franey',
#                                    'department_name': 'Purchasing',
#                                    'location': 'Manchester',
#                                    'email_address': 'jeremie.franey@terrifictotes.com'
#                                    }
