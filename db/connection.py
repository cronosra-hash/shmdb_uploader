import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool
from config.settings import DB_CONFIG

pool = SimpleConnectionPool(minconn=1, maxconn=10, **DB_CONFIG)


def get_connection():
    conn = pool.getconn()

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
        return conn
    except Exception:
        # Bad connection → close and replace with a new pooled one
        pool.putconn(conn, close=True)
        return pool.getconn()


def release_connection(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
        pool.putconn(conn)
    except Exception:
        pool.putconn(conn, close=True)


def get_dict_cursor(conn):
    """
    Returns a RealDictCursor with prepared statements disabled.
    """
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.prepare_threshold = 0
    return cursor
