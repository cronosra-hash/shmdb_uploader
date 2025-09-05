from datetime import datetime
import json  # Only needed if you're passing dicts into context


def log_update(
    cur,
    content_id,
    content_title,
    content_type,
    update_type,
    field_name,
    previous_value=None,
    current_value=None,
    context=None,
    source="uploader",
    timestamp=None,
):
    query = """
        INSERT INTO update_logs (
            content_id, content_title, content_type, update_type,
            field_name, previous_value, current_value,
            context, source, timestamp
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    values = (
        int(content_id),
        str(content_title),
        str(content_type),
        str(update_type),
        str(field_name),
        str(previous_value) if previous_value is not None else None,
        str(current_value) if current_value is not None else None,
        json.dumps(context) if context is not None else None,
        str(source),
        timestamp or datetime.utcnow(),
    )

    print("🧪 Logging update:", {
        "content_id": content_id,
        "field_name": field_name,
        "old": previous_value,
        "new": current_value
    })

    try:
        cur.execute(query, values)
        print("✅ Log inserted.")
    except Exception as e:
        print(f"❌ Log insert failed: {e}")
