from contextlib import contextmanager
import psycopg2.extras
from db.connection import get_connection, release_connection

@contextmanager
def dict_cursor():
    """
    Provides a RealDictCursor with prepared statements enabled,
    and guarantees the connection is returned to the pool.
    """
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.prepare_threshold = 0  # Enable prepared statements
        yield cursor
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        release_connection(conn)
