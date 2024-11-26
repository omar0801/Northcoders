from src.process_data import *
import json, pandas, pytest, pprint, os


design_JSON = [{"design_id": 8, "created_at": "2022-11-03T14:20:49.962000", "design_name": "Wooden", "file_location": "/usr", "file_name": "wooden-20220717-npgz.json", "last_updated": "2022-11-03T14:20:49.962000"}, {"design_id": 51, "created_at": "2023-01-12T18:50:09.935000", "design_name": "Bronze", "file_location": "/private", "file_name": "bronze-20221024-4dds.json", "last_updated": "2023-01-12T18:50:09.935000"}, {"design_id": 69, "created_at": "2023-02-07T17:31:10.093000", "design_name": "Bronze", "file_location": "/lost+found", "file_name": "bronze-20230102-r904.json", "last_updated": "2023-02-07T17:31:10.093000"}]

counterparty_JSON = [
  {
    "counterparty_id": 1,
    "counterparty_legal_name": "Fahey and Sons",
    "legal_address_id": 1,
    "commercial_contact": "Micheal Toy",
    "delivery_contact": "Mrs. Lucy Runolfsdottir",
    "created_at": "2022-11-03T14:20:51.563000",
    "last_updated": "2022-11-03T14:20:51.563000"
  },
  {
    "counterparty_id": 2,
    "counterparty_legal_name": "Leannon, Predovic and Morar",
    "legal_address_id": 2,
    "commercial_contact": "Melba Sanford",
    "delivery_contact": "Jean Hane III",
    "created_at": "2022-11-03T14:20:51.563000",
    "last_updated": "2022-11-03T14:20:51.563000"
  },
  {
    "counterparty_id": 3,
    "counterparty_legal_name": "Armstrong Inc",
    "legal_address_id": 3,
    "commercial_contact": "Jane Wiza",
    "delivery_contact": "Myra Kovacek",
    "created_at": "2022-11-03T14:20:51.563000",
    "last_updated": "2022-11-03T14:20:51.563000"
  }]

address_JSON = [
  {
    "address_id": 1,
    "address_line_1": "6826 Herzog Via",
    "address_line_2": None,
    "district": "Avon",
    "city": "New Patienceburgh",
    "postal_code": "28441",
    "country": "Turkey",
    "phone": "1803 637401",
    "created_at": "2022-11-03T14:20:49.962000",
    "last_updated": "2022-11-03T14:20:49.962000"
  },
  {
    "address_id": 2,
    "address_line_1": "179 Alexie Cliffs",
    "address_line_2": None,
    "district": None,
    "city": "Aliso Viejo",
    "postal_code": "99305-7380",
    "country": "San Marino",
    "phone": "9621 880720",
    "created_at": "2022-11-03T14:20:49.962000",
    "last_updated": "2022-11-03T14:20:49.962000"
  },
  {
    "address_id": 3,
    "address_line_1": "148 Sincere Fort",
    "address_line_2": None,
    "district": None,
    "city": "Lake Charles",
    "postal_code": "89360",
    "country": "Samoa",
    "phone": "0730 783349",
    "created_at": "2022-11-03T14:20:49.962000",
    "last_updated": "2022-11-03T14:20:49.962000"
  }]

# returns dictionary with keys dataframe and table_name
# table name should = dim design
# data frame should returns pandas dataframe
# data frame has correct collumns
# data frame has correct data
class TestDimDesign:
    def test_returns_dict_with_correct_keys(self):
        result = create_dim_design(design_JSON)
        assert isinstance(result, dict)
        assert 'dataframe' in result.keys()
        assert 'table_name' in result.keys()

    def test_table_name_correct(self):
        result = create_dim_design(design_JSON)
        assert result['table_name'] == 'dim_design'

    def test_design_returns_pd_dataframe(self):
        result = create_dim_design(design_JSON)
        assert type(result['dataframe']) == pandas.DataFrame

    def test_design_df_has_correct_columns(self):
        result = create_dim_design(design_JSON)['dataframe']
        assert list(result.columns.values) == ['design_id', 'design_name', 'file_location', 'file_name']

    def test_design_df_has_correct_data(self):
        result = create_dim_design(design_JSON)['dataframe']
        first_line = result.loc[0]
        assert first_line.to_dict() == {"design_id": 8,  "design_name": "Wooden", "file_location": "/usr", "file_name": "wooden-20220717-npgz.json"}


class TestDimCounterparty:
    def test_returns_dict_with_correct_keys(self):
        result = create_dim_counterparty(counterparty_JSON, address_JSON)
        assert isinstance(result, dict)
        assert 'dataframe' in result.keys()
        assert 'table_name' in result.keys()

    def test_table_name_correct(self):
        result = create_dim_counterparty(counterparty_JSON, address_JSON)['table_name']
        assert result == 'dim_counterparty'

    def test_counterparty_returns_pd_dataframe(self):
        result = create_dim_counterparty(counterparty_JSON, address_JSON)['dataframe']
        assert type(result) == pandas.DataFrame

    def test_counterparty_df_has_correct_columns(self):
        result = create_dim_counterparty(counterparty_JSON, address_JSON)['dataframe']
        assert list(result.columns.values) ==  ['counterparty_id',
                                                'counterparty_legal_name',
                                                'counterparty_legal_address_line_1',
                                                'counterparty_legal_address_line_2',
                                                'counterparty_legal_district',
                                                'counterparty_legal_city',
                                                'counterparty_legal_postal_code',
                                                'counterparty_legal_country',
                                                'counterparty_legal_phone_number']
        
    def test_counterparty_df_has_correct_data(self):
        result = create_dim_counterparty(counterparty_JSON, address_JSON)['dataframe']
        first_line = result.loc[0]
        assert first_line.to_dict() == {'counterparty_id': 1,
                                        'counterparty_legal_address_line_1': '6826 Herzog Via',
                                        'counterparty_legal_address_line_2': None,
                                        'counterparty_legal_city': 'New Patienceburgh',
                                        'counterparty_legal_country': 'Turkey',
                                        'counterparty_legal_district': 'Avon',
                                        'counterparty_legal_name': 'Fahey and Sons',
                                        'counterparty_legal_phone_number': '1803 637401',
                                        'counterparty_legal_postal_code': '28441',
                                        }
        

staff_json = [{"staff_id": 1, "first_name": "Jeremie", "last_name": "Franey", "department_id": 2, "email_address": "jeremie.franey@terrifictotes.com", "created_at": "2022-11-03T14:20:51.563000", "last_updated": "2022-11-03T14:20:51.563000"}, {"staff_id": 2, "first_name": "Deron", "last_name": "Beier", "department_id": 6, "email_address": "deron.beier@terrifictotes.com", "created_at": "2022-11-03T14:20:51.563000", "last_updated": "2022-11-03T14:20:51.563000"}]
    
dept_json = [{"department_id": 2, "department_name": "Purchasing", "location": "Manchester", "manager": "Naomi Lapaglia", "created_at": "2022-11-03T14:20:49.962000", "last_updated": "2022-11-03T14:20:49.962000"},  {
    "department_id": 6,
    "department_name": "Facilities",
    "location": "Manchester",
    "manager": "Shelley Levene",
    "created_at": "2022-11-03T14:20:49.962000",
    "last_updated": "2022-11-03T14:20:49.962000"
  }]

class TestDimStaff():

    def test_returns_dict_with_correct_keys(self):
        result = create_dim_staff(staff_json, dept_json)
        assert isinstance(result, dict)
        assert 'dataframe' in result.keys()
        assert 'table_name' in result.keys()

    def test_table_name_correct(self):
        result = create_dim_staff(staff_json, dept_json)
        assert result['table_name'] == 'dim_staff'

        
    def test_dim_staff_table_returns_pd_dataframe(self):
        response = create_dim_staff(staff_json, dept_json)
        assert type(response['dataframe']) == pd.DataFrame
        
    def test_dim_staff_df_has_correct_columns(self):
        response = create_dim_staff(staff_json, dept_json)
        assert list(response['dataframe'].columns.values) == ['staff_id',
                                            'first_name',
                                            'last_name',
                                            'department_name',
                                            'location',
                                            'email_address']
    def test_design_df_has_correct_data(self):
        response = create_dim_staff(staff_json, dept_json)
        first_row = response['dataframe'].iloc[0]
        assert first_row.to_dict() == {'staff_id': 1, 
                                    'first_name': 'Jeremie',
                                    "last_name": 'Franey',
                                    'department_name': 'Purchasing',
                                    'location': 'Manchester',
                                    'email_address': 'jeremie.franey@terrifictotes.com'
                                    }
        
currency_data = [{"currency_id": 1, "currency_code": "GBP", "created_at": "2022-11-03T14:20:49.962000", "last_updated": "2022-11-03T14:20:49.962000"}, {"currency_id": 2, "currency_code": "USD", "created_at": "2022-11-03T14:20:49.962000", "last_updated": "2022-11-03T14:20:49.962000"}, {"currency_id": 3, "currency_code": "EUR", "created_at": "2022-11-03T14:20:49.962000", "last_updated": "2022-11-03T14:20:49.962000"}]

class TestDimCurrency():
    def test_returns_dict_with_correct_keys(self):
        result = create_dim_currency(currency_data)
        assert isinstance(result, dict)
        assert 'dataframe' in result.keys()
        assert 'table_name' in result.keys()

    def test_table_name_correct(self):
        result = create_dim_currency(currency_data)
        assert result['table_name'] == 'dim_currency'

    def test_function_returns_df(self):

        result = create_dim_currency(currency_data)
        assert type(result['dataframe']) == pd.DataFrame

    def test_dataframe_contents_correct(self):
        currency_data = [{"currency_id": 1, "currency_code": "GBP", "created_at": "2022-11-03T14:20:49.962000", "last_updated": "2022-11-03T14:20:49.962000"}, {"currency_id": 2, "currency_code": "USD", "created_at": "2022-11-03T14:20:49.962000", "last_updated": "2022-11-03T14:20:49.962000"}, {"currency_id": 3, "currency_code": "EUR", "created_at": "2022-11-03T14:20:49.962000", "last_updated": "2022-11-03T14:20:49.962000"}]

        result = create_dim_currency(currency_data)['dataframe']
        first_line = result.loc[0]
        assert first_line.to_dict() == {"currency_id": 1, "currency_code": "GBP", "currency_name": "Great British Pounds"}

        first_line = result.loc[1]
        assert first_line.to_dict() == {"currency_id": 2, "currency_code": "USD", "currency_name": "US Dollars"}

        first_line = result.loc[2]
        assert first_line.to_dict() == {"currency_id": 3, "currency_code": "EUR", "currency_name": "Euros"}


class TestDimDate():
    def test_on_correct_date_format(self):
        start_date = '2000-01-01'
        end_date='2000-12-31'
        df = create_dim_date(start_date, end_date)
        df_value = df['dataframe'].loc[0]['date_id']
        df_to_date = pd.Timestamp(df_value)
        output = df_to_date.strftime('%Y-%m-%d')
        assert output == '2000-01-01'
    
    def test_on_in_correct_start_date_format(self):
        
        start_date = '200001-01'
        end_date='2000-12-31'
        output = create_dim_date(start_date, end_date)
        assert output == "Incorrect Date"
        
    def test_on_in_correct_end_date_format(self):
        
        start_date = '2000-01-01'
        end_date='200012-31'
        output = create_dim_date(start_date, end_date)
        assert output == "Incorrect Date"
        
    def test_on_start_date_and_end_date_other_way_round(self):
        start_date = '2000-01-01'
        end_date='1900-12-31'
        output = create_dim_date(start_date, end_date)
        assert output == "End date must be greter then Start date"

@pytest.fixture
def records():
    address_data = [
      {
            "address_id": 1,
            "address_line_1": "6826 Herzog Via",
            "address_line_2": None,
            "district": "Avon",
            "city": "New Patienceburgh",
            "postal_code": "28441",
            "country": "Turkey",
            "phone": "1803 637401",
            "created_at": "2022-11-03T14:20:49.962000",
            "last_updated": "2022-11-03T14:20:49.962000"
      },
      {
            "address_id": 2,
            "address_line_1": "179 Alexie Cliffs",
            "address_line_2": None,
            "district": None,
            "city": "Aliso Viejo",
            "postal_code": "99305-7380",
            "country": "San Marino",
            "phone": "9621 880720",
            "created_at": "2022-11-03T14:20:49.962000",
            "last_updated": "2022-11-03T14:20:49.962000"
      }]
    return address_data

class TestCreateDimLocation:    
    def test_dataframe_contains_correct_columns(self, records):
        output = create_dim_location(records)
        column_names = list(output['dataframe'].columns)
        assert len(column_names) == 8
        assert 'location_id' in column_names
        assert 'address_line_1' in column_names
        assert 'address_line_2' in column_names
        assert 'district' in column_names
        assert 'city' in column_names
        assert 'postal_code' in column_names
        assert 'country' in column_names
        assert 'phone' in column_names

    def test_dataframe_contains_correct_values(self, records):
        output = create_dim_location(records)
        output = output['dataframe'].map(lambda x: None if pd.isna(x) else x)
        for i in range(len(records)):
            row_values = output.iloc[i]
            output_values = row_values.tolist()
            original_values = list(records[i].values())
            assert len(output_values) == len(original_values) - 2
            assert output_values[0] == original_values[0]
            assert output_values[1:8] == original_values[1:8]