import json
from connection import connect_to_db, close_db_connection
from datetime import datetime
from decimal import Decimal

tables = [
    "counterparty",
    "currency",
    "department",
    "design",
    "staff",
    "sales_order",
    "address",
    "payment",
    "purchase_order",
    "payment_type",
    "transaction"
]

def fetch_data_from_table(conn, table_name):
    query = f"SELECT * FROM {table_name};"
    
    result = conn.run(query)
    columns = [col["name"] for col in conn.columns]
    
    data = []
    for row in result:
        row_dict = {}
        for i, value in enumerate(row):
            if isinstance(value, datetime):
                row_dict[columns[i]] = value.isoformat()
            elif isinstance(value, Decimal):
                row_dict[columns[i]] = float(value)
            else:
                row_dict[columns[i]] = value
        data.append(row_dict)
    
    return data

def save_to_json(data, filename):
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)
        
def main():
    conn = connect_to_db()
    try:
        for table in tables:
            print(f"Extracting data from {table}...")
            data = fetch_data_from_table(conn, table)
            json_filename = f"{table}.json"
            save_to_json(data, json_filename)
            print(f"Data from {table} saved to {json_filename}.")
    finally:
       
        close_db_connection(conn)
        print("Database connection closed.")

if __name__ == "__main__":
    main()


