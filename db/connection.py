from psycopg2.pool import SimpleConnectionPool
from config.settings import DB_CONFIG
import psycopg2.extras

pool = SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    **DB_CONFIG
)

def get_connection():
    return pool.getconn()

def release_connection(conn):
    pool.putconn(conn)

def get_dict_cursor(conn):
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.prepare_threshold = 0 
    return cursor



""" import psycopg2
from config.settings import DB_CONFIG, DB_OPTIONS

def get_connection():
    if DB_OPTIONS:
        return psycopg2.connect(**DB_CONFIG, options=DB_OPTIONS)
    return psycopg2.connect(**DB_CONFIG)
 """