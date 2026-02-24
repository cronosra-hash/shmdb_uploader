# services/logs.py

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


def fetch_new_update_logs(conn, content_id, content_type, since):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT timestamp, update_type, field_name, previous_value, current_value
            FROM update_logs
            WHERE content_id = %s AND content_type = %s AND timestamp > %s
            ORDER BY timestamp DESC;
            """,
            (content_id, content_type, since),
        )
        logs = cur.fetchall()

    return [
        {
            "timestamp": log[0],
            "update_type": log[1],
            "field_name": log[2],
            "previous_value": log[3],
            "current_value": log[4],
        }
        for log in logs
    ]


def filter_changes(raw_changes):
    """
    Filters out changes where previous_value == current_value or both empty.
    Formats timestamp for display.
    """
    filtered = []

    for change in raw_changes:
        old = change.get("previous_value")
        new = change.get("current_value")

        # Skip empty/no-op changes
        if (old is None and new is None) or (
            str(old).strip() == "" and str(new).strip() == ""
        ):
            continue
        if str(old) == str(new):
            continue

        # Format timestamp
        ts = change.get("timestamp")
        if isinstance(ts, datetime):
            change["timestamp"] = ts.strftime("%Y-%m-%d %H:%M:%S")

        filtered.append(change)

    return filtered
