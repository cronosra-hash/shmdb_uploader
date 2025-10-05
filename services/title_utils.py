import psycopg2
from db.connection import get_connection

def get_related_titles(title_id: int, title_type: str):
    if title_type == "movie":
        query = """
        SELECT DISTINCT m.movie_id, m.movie_title, m.release_date, m.poster_path
        FROM movies m
        JOIN movie_cast mc ON mc.movie_id = m.movie_id
        WHERE mc.actor_id IN (
            SELECT actor_id FROM movie_cast WHERE movie_id = %s
        )
        OR m.movie_id IN (
            SELECT mg2.movie_id
            FROM movie_genres mg1
            JOIN movie_genres mg2 ON mg1.genre_id = mg2.genre_id
            WHERE mg1.movie_id = %s AND mg2.movie_id != %s
        )
        AND m.movie_id != %s
        ORDER BY m.release_date DESC
        LIMIT 12;
        """
        params = (title_id, title_id, title_id, title_id)

    elif title_type == "tv":
        query = """
        SELECT DISTINCT s.series_id, s.series_name, s.first_air_date, s.poster_path
        FROM series s
        JOIN series_cast sc ON sc.series_id = s.series_id
        WHERE sc.person_id IN (
            SELECT person_id FROM series_cast WHERE series_id = %s
        )
        OR s.series_id IN (
            SELECT sg2.series_id
            FROM series_genres sg1
            JOIN series_genres sg2 ON sg1.genre_id = sg2.genre_id
            WHERE sg1.series_id = %s AND sg2.series_id != %s
        )
        AND s.series_id != %s
        ORDER BY s.first_air_date DESC
        LIMIT 12;
        """
        params = (title_id, title_id, title_id, title_id)

    else:
        return []

    db = get_connection()
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute(query, params)
        return cursor.fetchall()
