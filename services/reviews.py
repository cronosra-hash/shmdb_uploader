# from db.connection import get_connection
# import psycopg2.extras

# def get_reviews_for_title(title_id: int):
#     query = """
#     SELECT
#         r.id AS review_id,
#         r.username,
#         r.rating,
#         r.review_text,
#         r.created_at
#     FROM reviews r
#     WHERE r.title_id = %s
#     ORDER BY r.created_at DESC
#     """
#     db = get_connection()
#     with db.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
#         cursor.execute(query, (title_id,))
#         return cursor.fetchall()
