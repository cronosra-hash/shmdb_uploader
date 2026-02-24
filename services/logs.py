from datetime import datetime

def get_previous_log_timestamp(conn, content_id, content_type):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(timestamp)
            FROM update_logs
            WHERE content_id = %s AND content_type = %s;
            """,
            (content_id, content_type),
        )
        result = cur.fetchone()
        return result[0] or datetime.min
