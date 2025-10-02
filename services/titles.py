from db.connection import get_connection
import psycopg2.extras


def get_title_by_id(title_id: int):
    query = """
        SELECT
            m.movie_id,
            m.movie_title,
            mm.release_year,
            m.runtime,
            m.vote_average,
            m.vote_count,
            g.genre_name,
            m.poster_path
        FROM movies m
        LEFT JOIN movie_genres mg ON mg.movie_id = m.movie_id
        LEFT JOIN genres g ON g.genre_id = mg.genre_id
        JOIN movie_metadata mm ON mm.movie_id = m.movie_id
        WHERE m.movie_id = %s
    """
    db = get_connection()
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute(query, (title_id,))
        rows = cursor.fetchall()

    if not rows:
        return None

    base = rows[0]

    # Normalize keys for template compatibility
    base["id"] = base["movie_id"]
    base["title"] = base["movie_title"]
    base["type"] = "movie"
    base["genres"] = list({row["genre_name"] for row in rows if row.get("genre_name")})

    # Optional: fallback guards
    base["release_year"] = base.get("release_year")
    base["runtime"] = base.get("runtime")
    base["vote_average"] = base.get("vote_average")
    base["vote_count"] = base.get("vote_count")
    base["poster_path"] = base.get("poster_path")

    return base

def get_series_by_id(series_id: int):
    query = """
        SELECT
            s.series_id,
            s.series_name,
            s.vote_average,
            s.vote_count,
            g.genre_name,
            s.poster_path,
            s.first_air_date,
            s.last_air_date
        FROM series s
        LEFT JOIN series_genres sg ON sg.series_id = s.series_id
        LEFT JOIN genres g ON g.genre_id = sg.genre_id
        WHERE s.series_id = %s
    """
    db = get_connection()
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute(query, (series_id,))
        rows = cursor.fetchall()

    if not rows:
        return None

    base = rows[0]

    # Normalize keys for template compatibility
    base["id"] = base["series_id"]
    base["title"] = base["series_name"]
    base["type"] = "tv"
    base["genres"] = list({row["genre_name"] for row in rows if row.get("genre_name")})

    # Optional: fallback guards
    base["vote_average"] = base.get("vote_average")
    base["vote_count"] = base.get("vote_count")
    base["poster_path"] = base.get("poster_path")
    base["first_air_date"] = base.get("first_air_date")

    return base



DATE_FIELDS = {"release_date", "first_air_date"}
NUMERIC_FIELDS = {"budget", "revenue", "runtime", "vote_count", "number_of_seasons", "number_of_seasons"}

def get_movie_titles_missing(field: str):
    column = MOVIE_FIELD_MAP.get(field)
    if not column:
        raise ValueError(f"Invalid field: {field}")

    if column in NUMERIC_FIELDS or column in DATE_FIELDS:
        condition = f"{column} IS NULL"
    else:
        condition = f"{column} IS NULL OR {column} = ''"

    query = f"""
        SELECT
	        movie_id,
            movie_title,
            overview,
            release_date,
			runtime,
            poster_path,
			original_language,
            status,
            imdb_id,
            budget,
            revenue
        FROM movies
        WHERE {condition}
    """
    db = get_connection()
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute(query)
        return cursor.fetchall()

MOVIE_FIELD_MAP = {
    "missing_overview": "overview",
    "missing_release_date": "release_date",
    "missing_runtime": "runtime",
    "missing_poster_path": "poster_path",
    "missing_original_language": "original_language",
    "missing_status": "status",
    "missing_imdb": "imdb_id",
    "missing_budget": "budget",
    "missing_revenue": "revenue",
}

def get_tv_titles_missing(field: str):
    column = TV_FIELD_MAP.get(field)
    if not column:
        raise ValueError(f"Invalid field: {field}")

    if column in NUMERIC_FIELDS or column in DATE_FIELDS:
        condition = f"{column} IS NULL"
    else:
        condition = f"{column} IS NULL OR {column} = ''"

    query = f"""
        SELECT
            series_id,
            series_name,
			overview,
			first_air_date,
			poster_path,
			original_language,
			status,
			imdb_id,
			number_of_seasons,
			number_of_episodes
        FROM series
        WHERE {condition}
    """
    db = get_connection()
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute(query)
        return cursor.fetchall()

TV_FIELD_MAP = {
    "missing_overview": "overview",
    "missing_first_air_date": "first_air_date",
    "missing_runtime": "episode_run_time",
    "missing_poster_path": "poster_path",
    "missing_original_language": "original_language",
    "missing_status": "status",
    "missing_imdb": "imdb_id",
    "missing_network": "network",
    "missing_number_of_episodes": "number_of_episodes",
    "missing_number_of_seasons": "number_of_seasons"
}
