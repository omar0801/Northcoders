
def check_additions(db_data, s3_data):

    change_log = {}
    if not db_data:
        change_log['message'] = 'No records found in database'
        return change_log
    
    last_record = s3_data[-1]
    last_id = list(last_record.values())[0]    
    temp_rec = []
    for rec in reversed(db_data):
        id_value = list(rec.values())[0]
        if id_value > last_id:
            temp_rec.append(rec)
            change_log['message'] = "Addition detected" 
        else:
            break
    
    temp_rec.reverse()
    if not temp_rec:
        change_log['message'] = "No addition found"
    change_log['records'] = temp_rec 
    
    
    return change_log
    
    pass

    