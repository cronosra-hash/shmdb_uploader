from contextlib import contextmanager
import psycopg2.extras
from db.connection import get_connection, release_connection

@contextmanager
def dict_cursor():
    conn = get_connection()
    cursor = None
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.prepare_threshold = 0
        yield cursor
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        release_connection(conn)

