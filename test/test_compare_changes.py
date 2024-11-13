from src.compare_changes import check_additions, check_deletions, check_changes
import pytest

s3_data = [
    {
        "currency_id": 1,
        "currency_code": "GBP",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    },
    {
        "currency_id": 2,
        "currency_code": "USD",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    },
    {
        "currency_id": 3,
        "currency_code": "EUR",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    }
]

db_data = [
    {
        "currency_id": 1,
        "currency_code": "GBP",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    },
    {
        "currency_id": 2,
        "currency_code": "USD",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    },
    {
        "currency_id": 3,
        "currency_code": "EUR",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    },
    {
    
        "currency_id": 4,
        "currency_code": "JPY",
        "created_at": "2020-11-03T14:20:49.962001",
        "last_updated": "2020-11-03T14:20:49.962001"
    },
]

class TestCheckAdditions():

    def test_detect_addition_to_database(self):
        result = check_additions(db_data, s3_data)
        
        assert result['message'] == "Addition detected"  
    
    
    def test_logs_added_record(self):
        result = check_additions(db_data, s3_data)
        
        added_dic = {
            "currency_id": 4,
            "currency_code": "JPY",
            "created_at": "2020-11-03T14:20:49.962001",
            "last_updated": "2020-11-03T14:20:49.962001"
        }
        
        assert added_dic in result['records']

    def test_logs_added_more_records(self):
        
        s3_data = [
        {
            "currency_id": 1,
            "currency_code": "GBP",
            "created_at": "2022-11-03T14:20:49.962000",
            "last_updated": "2022-11-03T14:20:49.962000"
        },
        {
            "currency_id": 2,
            "currency_code": "USD",
            "created_at": "2022-11-03T14:20:49.962000",
            "last_updated": "2022-11-03T14:20:49.962000"
        },
        ]
        
        added_dic = [{
            "currency_id": 3,
            "currency_code": "EUR",
            "created_at": "2022-11-03T14:20:49.962000",
            "last_updated": "2022-11-03T14:20:49.962000"
        },
        {
            "currency_id": 4,
            "currency_code": "JPY",
            "created_at": "2020-11-03T14:20:49.962001",
            "last_updated": "2020-11-03T14:20:49.962001"
        }]
        result = check_additions(db_data, s3_data)
        
        assert added_dic == result['records']
        
    def test_return_message_if_db_data_is_empty(self):
        db_data = []

        result = check_additions(db_data, s3_data)
        assert result['message'] == 'No records found in database'

class TestCheckDeletions():
    def test_detects_one_deleted_item(self):
        result = check_deletions(s3_data, db_data)
        assert result['message'] == 'Deletion detected'
        assert result['records'] == [4]
    
    def test_detects_two_deletions(self):
        s3_data = [
    {
        "currency_id": 1,
        "currency_code": "GBP",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    },
    {
        "currency_id": 2,
        "currency_code": "USD",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    },
    {
        "currency_id": 3,
        "currency_code": "EUR",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    },
    {
    
        "currency_id": 4,
        "currency_code": "JPY",
        "created_at": "2020-11-03T14:20:49.962001",
        "last_updated": "2020-11-03T14:20:49.962001"
    },]
        
        db_data = [
    {
        "currency_id": 1,
        "currency_code": "GBP",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    },
    {
    
        "currency_id": 4,
        "currency_code": "JPY",
        "created_at": "2020-11-03T14:20:49.962001",
        "last_updated": "2020-11-03T14:20:49.962001"
    },]
        result = check_deletions(db_data, s3_data)
        assert result['message'] == 'Deletion detected'
        assert result['records'] == [2, 3]

    def test_detects_all_deletions_from_empty_db(self):
        s3_data = [
    {
        "currency_id": 1,
        "currency_code": "GBP",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    },
    {
    
        "currency_id": 4,
        "currency_code": "JPY",
        "created_at": "2020-11-03T14:20:49.962001",
        "last_updated": "2020-11-03T14:20:49.962001"
    },]
        db_data = []
        result = check_deletions(db_data, s3_data)
        assert result['message'] == 'Deletion detected'
        assert result['records'] == [1, 4]

    def test_no_deletions_detected(self):
        s3_data = [
    {
        "currency_id": 1,
        "currency_code": "GBP",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    },
    {
        "currency_id": 2,
        "currency_code": "USD",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    },
    {
        "currency_id": 3,
        "currency_code": "EUR",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    },
    {
    
        "currency_id": 4,
        "currency_code": "JPY",
        "created_at": "2020-11-03T14:20:49.962001",
        "last_updated": "2020-11-03T14:20:49.962001"
    },]
        db_data = s3_data
        result = check_deletions(db_data, s3_data)
        assert result['message'] == 'No deletions detected'
        assert 'records' not in list(result.keys())

class TestCheckChanges():
    def test_detects_single_change(self):

        s3_data = [{
            "currency_id": 1,
            "currency_code": "GBP",
            "created_at": "2022-11-03T14:20:49.962000",
            "last_updated": "2022-11-03T14:20:49.962000"
        }]

        db_data = [{
            "currency_id": 1,
            "currency_code": "USD",
            "created_at": "2022-11-03T14:20:49.962000",
            "last_updated": "2022-11-03T14:20:49.962000"
        }]

        result = check_changes(db_data, s3_data)
        assert result['message'] == 'Changes detected'
        assert result['records'] == [{'id': 1, 
                                     'currency_code': 'USD'}]


    def test_detects_single_change_and_ignores_unchanged_record(self):
        s3_data = [
    {
        "currency_id": 1,
        "currency_code": "GBP",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    },
    {
        "currency_id": 2,
        "currency_code": "USD",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    }]
        
        db_data = [
    {
        "currency_id": 1,
        "currency_code": "EUR",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    },
    {
        "currency_id": 2,
        "currency_code": "USD",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    }]
        
        result = check_changes(db_data, s3_data)
        assert result['message'] == 'Changes detected'
        assert result['records'] == [{'id': 1, 
                                     'currency_code': 'EUR'}]

    def test_detects_multiple_changes_to_same_record(self):

        s3_data = [{
            "currency_id": 1,
            "currency_code": "GBP",
            "created_at": "2022-11-03T14:20:49.962000",
            "last_updated": "2022-11-03T14:20:49.962000"
        }]

        db_data = [{
            "currency_id": 1,
            "currency_code": "USD",
            "created_at": "2024-12-03T14:20:49.962000",
            "last_updated": "2022-11-03T14:20:49.962000"
        }]

        result = check_changes(db_data, s3_data)
        assert result['message'] == 'Changes detected'
        assert result['records'] == [{'id': 1,
                                      'currency_code': "USD",
                                      "created_at": "2024-12-03T14:20:49.962000"}]

    def test_detects_multiple_changes_to_multiple_records(self):
            s3_data = [
        {
            "currency_id": 1,
            "currency_code": "GBP",
            "created_at": "2022-11-03T14:20:49.962000",
            "last_updated": "2022-11-03T14:20:49.962000"
        },
        {
            "currency_id": 2,
            "currency_code": "USD",
            "created_at": "2022-11-03T14:20:49.962000",
            "last_updated": "2022-11-03T14:20:49.962000"
        }]
            
            db_data = [
        {
            "currency_id": 1,
            "currency_code": "EUR",
            "created_at": "2022-11-03T14:20:49.962000",
            "last_updated": "2023-11-03T14:20:49.962000"
        },
        {
            "currency_id": 2,
            "currency_code": "JPY",
            "created_at": "2024-11-03T14:20:49.962000",
            "last_updated": "2022-11-03T14:20:49.962000"
        }]
            
            result = check_changes(db_data, s3_data)
            assert result['message'] == 'Changes detected'
            assert result['records'] == [{'id': 1, 
                                        'currency_code': 'EUR',
                                        "last_updated": "2023-11-03T14:20:49.962000"},
                                        {'id': 2,
                                         'currency_code': 'JPY',
                                         'created_at': "2024-11-03T14:20:49.962000"}]
    
    def test_returns_message_if_no_changes_found(self):
        s3_data = [{
            "currency_id": 1,
            "currency_code": "GBP",
            "created_at": "2022-11-03T14:20:49.962000",
            "last_updated": "2022-11-03T14:20:49.962000"
        }]

        db_data = s3_data

        result = check_changes(db_data, s3_data)
        assert result['message'] == 'No changes detected'
        assert 'records' not in list(result.keys())
