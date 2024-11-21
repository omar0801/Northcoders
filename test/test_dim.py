from src.process_data import create_dim_design
from src.process_data import create_dim_counterparty
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
                                        'counterparty_legal_address_line_1': '6826 Herzog Via',
                                        'counterparty_legal_address_line_2': None,
                                        'counterparty_legal_city': 'New Patienceburgh',
                                        'counterparty_legal_country': 'Turkey',
                                        'counterparty_legal_district': 'Avon',
                                        'counterparty_legal_name': 'Fahey and Sons',
                                        'counterparty_phone_number': '1803 637401',
                                        'counterparty_postal_code': '28441',
                                        }
        