from db.connection import get_connection, release_connection
from tmdb.movie_api import get_movie_data
from uploader.movie_uploader import insert_or_update_movie_data

MOVIE_IDS = [1078605, 347969]

def main():
    conn = get_connection()
    try:
        for movie_id in MOVIE_IDS:
            print(f"🔄 Fetching movie ID {movie_id}...")
            movie_data = get_movie_data(movie_id)
            if movie_data is None:
                continue
            try:
                insert_or_update_movie_data(conn, movie_data)
            except Exception as e:
                print(f"❌ Error processing movie ID {movie_id}: {e}")
                conn.rollback()
    finally:
        release_connection(conn)

if __name__ == "__main__":
    main()
