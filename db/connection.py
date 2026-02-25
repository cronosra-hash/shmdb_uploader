import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool
from config.settings import DB_CONFIG

pool = SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    **DB_CONFIG
)

def get_connection():
    """
    Returns a valid, working connection.
    If the connection is stale or closed, it is replaced.
    """
    conn = pool.getconn()

    try:
        # Test the connection
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
    except Exception:
        # Connection is dead → replace it
        try:
            pool.putconn(conn, close=True)
        except Exception:
            pass

        conn = psycopg2.connect(**DB_CONFIG)

    return conn


def release_connection(conn):
    """
    Safely return a connection to the pool.
    If it's broken, close it instead.
    """
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




""" import psycopg2
from config.settings import DB_CONFIG, DB_OPTIONS

def get_connection():
    if DB_OPTIONS:
        return psycopg2.connect(**DB_CONFIG, options=DB_OPTIONS)
    return psycopg2.connect(**DB_CONFIG)
 """