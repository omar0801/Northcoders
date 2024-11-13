from src.compare_changes import check_additions


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

def test_detect_addition_to_database():
    result = check_additions(db_data, s3_data)
    
    assert result['message'] == "Addition detected"  
    
    
def test_logs_added_record():
    result = check_additions(db_data, s3_data)
    
    added_dic = {
        "currency_id": 4,
        "currency_code": "JPY",
        "created_at": "2020-11-03T14:20:49.962001",
        "last_updated": "2020-11-03T14:20:49.962001"
    }
    
    assert added_dic in result['records']

def test_logs_added_more_records():
    
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
    
def test_return_message_if_db_data_is_empty():
    db_data = []

    result = check_additions(db_data, s3_data)
    assert result['message'] == 'No records found in database'