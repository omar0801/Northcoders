import pg8000.native
from dotenv import load_dotenv
import os

load_dotenv(override=True)
def connect_to_db():
    return pg8000.native.Connection(
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        database=os.getenv("PG_DATABASE"),
        host=os.getenv("PG_HOST"),
        port=int(os.getenv("PG_PORT"))
    )

def close_db_connection(conn):
    conn.close()




