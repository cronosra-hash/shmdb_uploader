def log_update(cur, item_id, item_title, update_type, field_name=None, old_value=None, new_value=None):
    query = """
    INSERT INTO update_logs (movie_id, movie_title, update_type, field_name, old_value, new_value)
    VALUES (%s, %s, %s, %s, %s, %s);
    """
    values = (
        item_id,
        item_title,
        update_type,
        field_name,
        str(old_value) if old_value is not None else None,
        str(new_value) if new_value is not None else None,
    )
    cur.execute(query, values)
