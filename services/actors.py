from db.connection import get_connection
import psycopg2.extras


def get_cast_for_title(title_id: int):
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
    db = get_connection()
    with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        cursor.execute(query, (title_id,))
        return cursor.fetchall()
