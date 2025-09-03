from datetime import datetime

def log_update(cur, content_id, content_title, content_type, update_type, updated_field=None, previous_value=None, current_value=None):
    query = """
        INSERT INTO update_logs (
            content_id, content_title, content_type, update_type,
            updated_field, previous_value, current_value, logged_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """

    values = (
        str(content_id),
        str(content_title),
        str(content_type),  # ✅ This was missing
        str(update_type),
        str(updated_field) if updated_field is not None else None,
        str(previous_value) if previous_value is not None else None,
        str(current_value) if current_value is not None else None,
        datetime.utcnow(),
    )

    try:
        cur.execute(query, values)
        print("✅ Log inserted.")
    except Exception as e:
        print(f"❌ Log insert failed: {e}")

