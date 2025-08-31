from db.connection import get_connection
import psycopg2.extras


def get_title_by_id(title_id: int):
    query = """
        SELECT
        m.id,
        m.title,
        mad.release_year,
        m.runtime,
        m.vote_average,
        m.vote_count,
        g.name,
        m.poster_path
    FROM movies m
    LEFT JOIN movie_genres mg ON mg.movie_id = m.id
    LEFT JOIN genres g ON g.id = mg.genre_id
	JOIN movie_additional_details mad ON mad.movie_id = m.id
    WHERE m.id = %s
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
