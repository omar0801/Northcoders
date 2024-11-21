from src.dim_design import create_dim_design
from src.dim_counterparty import create_dim_counterparty
import json, pandas, pytest, pprint

with open('/home/michael/Northcoders/de-project-specification/data/2024-11-18T13_37_12.390620.json', 'r') as design_json_file:
    design_JSON = json.load(design_json_file)

with open('/home/michael/Northcoders/de-project-specification/data/counterparty_2.json', 'r') as counterparty_json_file:
    counterparty_JSON = json.load(counterparty_json_file)

with open('/home/michael/Northcoders/de-project-specification/data/address_2.json', 'r') as address_json_file:
    address_JSON = json.load(address_json_file)


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
    def test_counterparty_returns_pd_dataframe(self):
        result = create_dim_counterparty(counterparty_JSON, address_JSON)
        assert type(result) == pandas.DataFrame

    def test_counterparty_df_has_correct_columns(self):
        result = create_dim_counterparty(counterparty_JSON, address_JSON)
        assert list(result.columns.values) ==  ['counterparty_id',
                                                'counterparty_legal_name',
                                                'counterparty_legal_address_line_1',
                                                'counterparty_legal_address_line_2',
                                                'counterparty_legal_district',
                                                'counterparty_legal_city',
                                                'counterparty_postal_code',
                                                'counterparty_legal_country',
                                                'counterparty_phone_number']
        
    def test_counterparty_df_has_correct_data(self):
        result = create_dim_counterparty(counterparty_JSON, address_JSON)
        first_line = result.loc[0]
        assert first_line.to_dict() == {'counterparty_id': 1,
                                        'counterparty_legal_address_line_1': '605 Haskell Trafficway',
                                        'counterparty_legal_address_line_2': 'Axel Freeway',
                                        'counterparty_legal_city': 'East Bobbie',
                                        'counterparty_legal_country': 'Heard Island and McDonald Islands',
                                        'counterparty_legal_district': None,
                                        'counterparty_legal_name': 'Fahey and Sons',
                                        'counterparty_phone_number': '9687 937447',
                                        'counterparty_postal_code': '88253-4257',
                                        }