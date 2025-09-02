from datetime import datetime

def log_update(cur, movie_id, movie_title, update_type, updated_field=None, previous_value=None, current_value=None):
    query = """
        INSERT INTO update_logs (
            movie_id, movie_title, update_type,
            updated_field, previous_value, current_value, logged_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """

    values = (
        str(movie_id),
        str(movie_title),
        str(update_type),
        str(updated_field) if updated_field is not None else None,
        str(previous_value) if previous_value is not None else None,
        str(current_value) if current_value is not None else None,
        datetime.utcnow(),
    )

    print(f"📦 Values tuple: {values} (length={len(values)})")

    try:
        cur.execute(query, values)
        print("✅ Log inserted.")
    except Exception as e:
        print(f"❌ Log insert failed: {e}")
