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
    
    if not temp_rec:
        change_log['message'] = "No addition found"
    
    temp_rec.reverse()
    change_log['records'] = temp_rec 
    
    return change_log
    

def check_deletions(db_data, s3_data):
    
    delete_log = {}

    s3_id_list = [list(rec.values())[0] for rec in s3_data]
    db_id_list = [list(rec.values())[0] for rec in db_data]

    deleted_list = []
    for item in s3_id_list:
        if item not in db_id_list:
            deleted_list.append(item)
            delete_log['message'] = 'Deletion detected'
    
    if deleted_list:
        delete_log['records'] = deleted_list
    else:
        delete_log['message'] = 'No deletions detected'

    return delete_log
    

def check_changes(db_data, s3_data):
    change_log = {}

    s3_dict = {list(rec.values())[0]: rec for rec in s3_data}
    db_dict = {list(rec.values())[0]: rec for rec in db_data}
    keys = list(s3_data[0].keys())

    changed_recs = []
    for id, data in db_dict.items():
        if s3_dict[id] != data:
            changed_rec = {'id': id}
            for key in keys:
                if s3_dict[id][key] != data[key]:
                    changed_rec[key] = data[key]
            changed_recs.append(changed_rec)

    if changed_recs:
        change_log['message'] = 'Changes detected'
        change_log['records'] = changed_recs  
    else:
        change_log['message'] = 'No changes detected'
        
    return change_log