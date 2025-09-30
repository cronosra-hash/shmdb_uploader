from db.connection import get_connection
import psycopg2.extras


def get_cast_for_title(title_id: int, title_type: str):
    if title_type == "movie":
        query = """
        SELECT
            mc.actor_id,
            p.name,
            p.profile_path,
            mc.character_name
        FROM movie_cast mc
        JOIN people p ON p.person_id = mc.actor_id
        WHERE mc.movie_id = %s
        ORDER BY mc.cast_order ASC
        """
    elif title_type == "tv":
        query = """
        SELECT
            sc.person_id,
            p.name,
            p.profile_path,
            sc.character_name
        FROM series_cast sc
        JOIN people p ON p.person_id = sc.person_id
        WHERE sc.series_id = %s
        ORDER BY sc.cast_order ASC
        """
    else:
        return []

    db = get_connection()
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute(query, (title_id,))
        return cursor.fetchall()

