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

def get_new_releases():
    # Static scaffold — replace with scraping/API logic later
    return [
        {
            "title": "Downton Abbey: The Grand Finale",
            "release_date": "2025-09-05",
            "genre": "Drama",
            "source": "Movie Insider",
            "source_url": "https://www.movieinsider.com/movies/september/2025",
        },
        {
            "title": "Warfare",
            "release_date": "2025-09-12",
            "genre": "War/Drama",
            "source": "HBO Max",
            "source_url": "https://www.msn.com/en-us/entertainment/tv/hbo-max-to-add-12-new-shows-movies-this-week-here-are-the-3-to-watch/ar-AA1M3OfG",
        },
        {
            "title": "One Battle After Another",
            "release_date": "2025-09-TBD",
            "genre": "Thriller",
            "source": "Radio Times",
            "source_url": "https://www.radiotimes.com/movies/best-films-uk-september-2025/",
        },
    ]
