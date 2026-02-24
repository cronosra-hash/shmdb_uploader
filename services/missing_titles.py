from db.connection import get_connection, release_connection
import psycopg2.extras

from db.helpers import dict_cursor

def get_titles_missing(field: str):
    column = FIELD_MAP.get(field)
    if not column:
        raise ValueError(f"Invalid field: {field}")

    query = f"""
        SELECT
            m.movie_id,
            m.movie_title,
            m.poster_path,
            m.runtime,
            m.original_language,
            m.status,
            m.imdb_id,
            m.budget,
            m.revenue,
            mm.release_year,
            m.tagline
        FROM movies m
        LEFT JOIN movie_metadata mm ON mm.movie_id = m.movie_id
        WHERE m.{column} IS NULL OR m.{column} = ''
    """
    with dict_cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()

FIELD_MAP = {
    "missing_overview": "overview",
    "missing_release_date": "release_date",
    "missing_runtime": "runtime",
    "missing_poster_path": "poster_path",
    "missing_original_language": "original_language",
    "missing_status": "status",
    "missing_imdb": "imdb_id",
    "missing_budget": "budget",
    "missing_revenue": "revenue",
    "missing_tagline": "tagline"
}
