from pathlib import Path
from zoneinfo import ZoneInfo

from db.helpers import dict_cursor

QUERIES_DIR = Path(__file__).parent.parent / "queries"

def load_query(name: str) -> str:
    return (QUERIES_DIR / f"{name}.sql").read_text()

def fetch_stat(query_name: str):
    query = load_query(query_name)
    with dict_cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()

# Individual stat loaders
def get_top_rated_movies():
    return fetch_stat("top_rated_movies")

def get_most_reviewed_titles():
    return fetch_stat("most_reviewed_titles")

def get_prolific_actors():
    return fetch_stat("prolific_actors")

def get_top_rated_actors():
    return fetch_stat("top_rated_actors")

def get_active_release_years():
    return fetch_stat("active_release_years")

def get_trending_titles():
    return fetch_stat("trending_titles")

def get_popular_genres():
    return fetch_stat("popular_genres")

def get_hidden_gems():
    return fetch_stat("hidden_gems")

def get_all_stats():
    query = load_query("all_stats")
    with dict_cursor() as cursor:
        cursor.execute(query)
        row = cursor.fetchone()
        return row["stats"]

#  Statistics Page
def get_new_releases():
    return []

def get_top_fields():
    return fetch_stat("top_fields")

def get_movie_count():
    result = fetch_stat("movie_count")
    return result[0]["count"]

def get_series_count():
    result = fetch_stat("series_count")
    return result[0]["count"]

def get_last_update():
    result = fetch_stat("last_update")
    if result:
        dt = result[0]["max"]
        return dt.astimezone(ZoneInfo("Europe/London"))
    return None

def get_recent_updates():
    result = fetch_stat("recent_updates")
    return result[0]["recent_updates"]

def get_most_updated_title():
    result = fetch_stat("most_updated_title")
    return {
    "most_updated_title": result[0]["most_updated_title"],
    "title_type": result[0]["title_type"],
    "title_changes": result[0]["title_changes"]
} if result else None

def get_movies_missing_fields():
    result = fetch_stat("movies_missing_fields")
    return {
        "missing_overview": result[0]["missing_overview"],
        "missing_release_date": result[0]["missing_release_date"],
        "missing_runtime": result[0]["missing_runtime"],
        "missing_poster_path": result[0]["missing_poster_path"],
        "missing_original_language": result[0]["missing_original_language"],
        "missing_status": result[0]["missing_status"],
        "missing_imdb": result[0]["missing_imdb"],
        "missing_budget": result[0]["missing_budget"],
        "missing_revenue": result[0]["missing_revenue"],
        "missing_tagline": result[0]["missing_tagline"]
    } if result else None

def get_series_missing_fields():
    result = fetch_stat("series_missing_fields")
    return {
        "missing_overview": result[0]["missing_overview"],
        "missing_first_air_date": result[0]["missing_first_air_date"],
        "missing_poster_path": result[0]["missing_poster_path"],
        "missing_original_language": result[0]["missing_original_language"],
        "missing_status": result[0]["missing_status"],
        "missing_imdb": result[0]["missing_imdb"],
        "missing_seasons": result[0]["missing_seasons"],
        "missing_episodes": result[0]["missing_episodes"]
    } if result else None

def get_orphaned_logs():
    result = fetch_stat("orphaned_logs")
    return result[0].get("orphaned_logs", 0) if result else 0
