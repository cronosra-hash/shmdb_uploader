from db.connection import get_connection, release_connection
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import psycopg2.extras

from db.helpers import dict_cursor

def classify_freshness(last_updated):
    if not last_updated:
        return "stale"
    now = datetime.utcnow()
    delta = now - last_updated
    if delta <= timedelta(days=7):
        return "fresh"
    elif delta <= timedelta(days=30):
        return "moderate"
    else:
        return "stale"

def format_local(dt, fmt="%d %b %Y, %H:%M"):
    if not isinstance(dt, datetime):
        return "Unknown"
    return dt.astimezone(ZoneInfo("Europe/London")).strftime(fmt)

def get_freshness_summary():
    query = """
        SELECT movie_title, last_updated FROM movies WHERE last_updated IS NOT NULL;
    """
    with dict_cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    results = []
    for row in rows:
        category = classify_freshness(row["last_updated"])
        results.append({
            "title": row["movie_title"],
            "freshness": category,
            "last_updated": format_local(row["last_updated"])
        })

    return results
