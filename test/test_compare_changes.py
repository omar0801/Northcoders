from src.ingestion import check_additions, check_deletions, check_changes, main_check_for_changes
import pytest, boto3, json
from moto import mock_aws
from unittest.mock import Mock, patch
import pprint as pp


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

    def test_return_message_if_s3_data_is_empty(self):
        s3_data = []

        result = check_additions(db_data, s3_data)
        assert result['records'] == 'no s3 data'

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
    
    def test_for_empty_s3(self):
        s3_data = []

        result = check_deletions(db_data, s3_data)
        assert result['records'] == 'no s3 data'

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
    
    def test_check_changes_for_empty_s3_(self):
        s3_data = []

        result = check_changes(db_data, s3_data)
        assert result['records'] == 'no s3 data'



@pytest.fixture
def records():

    


    s3_payment_type_records = [{
        "payment_type_id": 1,
        "payment_type_name": "SALES_RECEIPT",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    },
    {
        "payment_type_id": 2,
        "payment_type_name": "SALES_REFUND",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    }]

    s3_currency_records = [{
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

    db_payment_type_records = [{
        "payment_type_id": 1,
        "payment_type_name": "SALES_RECEIPT",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    },
    {
        "payment_type_id": 2,
        "payment_type_name": "SALES_REFUND",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    }]
    db_currency_records = [{
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



    combined_dict_records = {
        "s3":{"currency": s3_currency_records, "payment":s3_payment_type_records}, 
        "db":{"currency": db_currency_records, "payment":db_payment_type_records}
        }
    
    return combined_dict_records
    

@pytest.fixture()
def s3_mock():
    with mock_aws():
        s3 = boto3.client('s3')
        s3.create_bucket(
            Bucket='ingestion-bucket-neural-normalisers-new',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            }
        )
        yield s3

@pytest.fixture()
def datetime_mock():
    with patch('src.ingestion.datetime') as mock_dt:
        mock_dt.now.return_value.isoformat.return_value = ('mock_timestamp')
        yield mock_dt


class TestMainFunc():

    def test_check_for_no_changes(self, records, s3_mock, datetime_mock):
        results = main_check_for_changes(records, s3_mock)
        assert results == []

        s3_contents = s3_mock.list_objects_v2(
            Bucket='ingestion-bucket-neural-normalisers-new'
        )

        assert s3_contents['Contents'][0]['Key'] == 'changes_log/currency/mock_timestamp.json'
        assert s3_contents['Contents'][1]['Key'] == 'changes_log/payment/mock_timestamp.json'

        s3_mock_data = s3_mock.get_object(Bucket='ingestion-bucket-neural-normalisers-new',
            Key='changes_log/currency/mock_timestamp.json')
        s3_mock_data_body = s3_mock_data['Body'].read().decode('utf-8')
        s3_mock_dict = json.loads(s3_mock_data_body)
        

        assert s3_mock_dict == {'currency': {'additions': None, 'deletions': None, 'changes': None}}

        s3_mock_data = s3_mock.get_object(Bucket='ingestion-bucket-neural-normalisers-new',
            Key='changes_log/payment/mock_timestamp.json')
        s3_mock_data_body = s3_mock_data['Body'].read().decode('utf-8')
        s3_mock_dict = json.loads(s3_mock_data_body)

        assert s3_mock_dict == {'payment': {'additions': None, 'deletions': None, 'changes': None}}

    
    def test_for_additions(self, records, s3_mock, datetime_mock):
        records['db']['currency'].append({
        "currency_id": 3,
        "currency_code": "EUR",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    })
        
        result = main_check_for_changes(records, s3_mock)
        
        s3_contents = s3_mock.list_objects_v2(
            Bucket='ingestion-bucket-neural-normalisers-new'
        )

        assert result == ['currency']
    
        assert s3_contents['Contents'][0]['Key'] == 'changes_log/currency/mock_timestamp.json'
        assert s3_contents['Contents'][1]['Key'] == 'changes_log/payment/mock_timestamp.json'
        s3_mock_data = s3_mock.get_object(Bucket='ingestion-bucket-neural-normalisers-new',
                    Key='changes_log/currency/mock_timestamp.json')
        s3_mock_data_body = s3_mock_data['Body'].read().decode('utf-8')
        s3_mock_dict = json.loads(s3_mock_data_body)
            

        assert s3_mock_dict == {'currency': {'additions': [{
        "currency_id": 3,
        "currency_code": "EUR",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
    }], 'deletions': None, 'changes': None}}

        s3_mock_data = s3_mock.get_object(Bucket='ingestion-bucket-neural-normalisers-new',
                    Key='changes_log/payment/mock_timestamp.json')
        s3_mock_data_body = s3_mock_data['Body'].read().decode('utf-8')
        s3_mock_dict = json.loads(s3_mock_data_body)

        assert s3_mock_dict == {'payment': {'additions': None, 'deletions': None, 'changes': None}}

    def test_for_deleted_records(self, records, s3_mock, datetime_mock):
         
        records['db']['currency'].pop(-1)
        # pp.pprint(records)
        result = main_check_for_changes(records, s3_mock)
        
        s3_contents = s3_mock.list_objects_v2(
                Bucket='ingestion-bucket-neural-normalisers-new'
            )

        assert result == ['currency']
        
        assert s3_contents['Contents'][0]['Key'] == 'changes_log/currency/mock_timestamp.json'
        assert s3_contents['Contents'][1]['Key'] == 'changes_log/payment/mock_timestamp.json'

        s3_mock_data = s3_mock.get_object(Bucket='ingestion-bucket-neural-normalisers-new',
            Key='changes_log/currency/mock_timestamp.json')
        s3_mock_data_body = s3_mock_data['Body'].read().decode('utf-8')
        s3_mock_dict = json.loads(s3_mock_data_body)
        

        assert s3_mock_dict == {'currency': {'additions': None, 'deletions':[2], 'changes': None}}

        s3_mock_data = s3_mock.get_object(Bucket='ingestion-bucket-neural-normalisers-new',
            Key='changes_log/payment/mock_timestamp.json')
        s3_mock_data_body = s3_mock_data['Body'].read().decode('utf-8')
        s3_mock_dict = json.loads(s3_mock_data_body)

        assert s3_mock_dict == {'payment': {'additions': None, 'deletions': None, 'changes': None}}

        

    def test_for_updated_records(self, records, s3_mock, datetime_mock):
         
        records['db']['currency'][0]["currency_code"] = "JPY"
        result = main_check_for_changes(records, s3_mock)
        
        s3_contents = s3_mock.list_objects_v2(
                Bucket='ingestion-bucket-neural-normalisers-new'
            )

        assert result == ['currency']
        
        assert s3_contents['Contents'][0]['Key'] == 'changes_log/currency/mock_timestamp.json'
        assert s3_contents['Contents'][1]['Key'] == 'changes_log/payment/mock_timestamp.json'

        s3_mock_data = s3_mock.get_object(Bucket='ingestion-bucket-neural-normalisers-new',
            Key='changes_log/currency/mock_timestamp.json')
        s3_mock_data_body = s3_mock_data['Body'].read().decode('utf-8')
        s3_mock_dict = json.loads(s3_mock_data_body)
        

        assert s3_mock_dict == {'currency': {'additions': None, 'deletions': None, 'changes': [{"id": 1,
            "currency_code": "JPY"}]}}

        s3_mock_data = s3_mock.get_object(Bucket='ingestion-bucket-neural-normalisers-new',
            Key='changes_log/payment/mock_timestamp.json')
        s3_mock_data_body = s3_mock_data['Body'].read().decode('utf-8')
        s3_mock_dict = json.loads(s3_mock_data_body)

        assert s3_mock_dict == {'payment': {'additions': None, 'deletions': None, 'changes': None}}


    def test_for_multiple_changes_at_once(self, records, s3_mock, datetime_mock):
        records['db']['currency'].append({
        "currency_id": 3,
        "currency_code": "EUR",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
        })
        records['db']['currency'][0]["currency_code"] = "JPY"
        records['db']['payment'].pop(-1)

        result = main_check_for_changes(records, s3_mock)
        assert result == ['currency', 'payment']
        
        s3_mock_data = s3_mock.get_object(Bucket='ingestion-bucket-neural-normalisers-new',
            Key='changes_log/currency/mock_timestamp.json')
        s3_mock_data_body = s3_mock_data['Body'].read().decode('utf-8')
        s3_mock_dict = json.loads(s3_mock_data_body)
        

        assert s3_mock_dict == {'currency': {'additions': [{
        "currency_id": 3,
        "currency_code": "EUR",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
        }], 'deletions': None, 'changes': [{"id": 1,
            "currency_code": "JPY"}]}}

        s3_mock_data = s3_mock.get_object(Bucket='ingestion-bucket-neural-normalisers-new',
            Key='changes_log/payment/mock_timestamp.json')
        s3_mock_data_body = s3_mock_data['Body'].read().decode('utf-8')
        s3_mock_dict = json.loads(s3_mock_data_body)

        assert s3_mock_dict == {'payment': {'additions': None, 'deletions': [2], 'changes': None}}

    def test_addition_deletion_same_table(self, records, s3_mock, datetime_mock):
        records['db']['currency'].append({
        "currency_id": 3,
        "currency_code": "EUR",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
        })
        records['db']['currency'].pop(0)
        result = main_check_for_changes(records, s3_mock)
        assert result == ['currency']

        s3_mock_data = s3_mock.get_object(Bucket='ingestion-bucket-neural-normalisers-new',
            Key='changes_log/currency/mock_timestamp.json')
        s3_mock_data_body = s3_mock_data['Body'].read().decode('utf-8')
        s3_mock_dict = json.loads(s3_mock_data_body)
        

        assert s3_mock_dict == {'currency': {'additions': [{
        "currency_id": 3,
        "currency_code": "EUR",
        "created_at": "2022-11-03T14:20:49.962000",
        "last_updated": "2022-11-03T14:20:49.962000"
        }], 'deletions':[1], 'changes': None}}

        s3_mock_data = s3_mock.get_object(Bucket='ingestion-bucket-neural-normalisers-new',
            Key='changes_log/payment/mock_timestamp.json')
        s3_mock_data_body = s3_mock_data['Body'].read().decode('utf-8')
        s3_mock_dict = json.loads(s3_mock_data_body)

        assert s3_mock_dict == {'payment': {'additions': None, 'deletions': None, 'changes': None}}

    def test_no_data_for_s3(self, records, s3_mock, datetime_mock):
        def test_check_changes_for_empty_s3_(self):
            records['s3']['currency'] = []
            result = main_check_for_changes(records, s3_mock)

            assert result == ['currency', 'payment']
        
        








        

        
        

        

        


        




        
        


        

