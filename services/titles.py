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
    base["genres"] = list({row["name"] for row in rows if row["name"]})
    return base
