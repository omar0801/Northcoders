from src.connection import *
import os
from pg8000.native import Connection


def test_connection_to_db():
    conn = connect_to_db()
    assert isinstance(conn, Connection)
    close_db_connection(conn)
    

