import psycopg2.extras
from pathlib import Path
from sqlalchemy import text
from db.connection import get_connection

QUERIES_DIR = Path(__file__).parent.parent / "queries"

def load_query(name: str) -> str:
    return (QUERIES_DIR / f"{name}.sql").read_text()

def fetch_stat(query_name: str):
    query = load_query(query_name)
    db = get_connection()
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
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
