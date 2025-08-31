from db.connection import get_connection
from datetime import datetime, timedelta
import psycopg2.extras

def classify_freshness(lastupdated):
    if not lastupdated:
        return "stale"
    now = datetime.utcnow()
    delta = now - lastupdated
    if delta <= timedelta(days=7):
        return "fresh"
    elif delta <= timedelta(days=30):
        return "moderate"
    else:
        return "stale"

def get_freshness_summary():
    query = """
        SELECT lastupdated FROM movies WHERE lastupdated IS NOT NULL;
    """
    db = get_connection()
    with db.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    summary = {"fresh": 0, "moderate": 0, "stale": 0}
    for row in rows:
        category = classify_freshness(row["lastupdated"])
        summary[category] += 1

    return summary
