#!/usr/bin/env python3
"""
Backfill TMDb movie taglines into your database.
- Fetches movie details from TMDb.
- Inserts or updates the tagline field in the movies table.
"""

import psycopg2
import requests

TMDB_API_KEY = ""

def fetch_movie_details(movie_id, tmdb_api_key):
    """Fetch movie details from TMDb (includes tagline)."""
    url = f"https://api.themoviedb.org/3/movie/{movie_id}"
    params = {"api_key": tmdb_api_key}
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    return resp.json()

def backfill_movie_taglines(conn, tmdb_api_key):
    with conn.cursor() as cur:
        cur.execute("SELECT movie_id, movie_title FROM movies;")
        movie_rows = cur.fetchall()

    for movie_id, movie_title in movie_rows:
        print(f"🔄 Updating tagline for {movie_title} (ID={movie_id})")

        try:
            details = fetch_movie_details(movie_id, tmdb_api_key)
            tagline = details.get("tagline")

            if tagline and tagline.strip():
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE movies
                        SET tagline = %s,
                            last_updated = CURRENT_TIMESTAMP
                        WHERE movie_id = %s;
                        """,
                        (tagline.strip(), movie_id),
                    )
                conn.commit()
                print(f"✅ Tagline updated for {movie_title}: {tagline}")
            else:
                print(f"⚠️ No tagline found for {movie_title}")

        except Exception as e:
            print(f"❌ Failed to update {movie_title}: {e}")
            conn.rollback()

def diagnostic_summary(conn):
    """Print a summary of movies with and without taglines."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FILTER (WHERE tagline IS NOT NULL AND tagline <> '') AS with_tagline,
                   COUNT(*) FILTER (WHERE tagline IS NULL OR tagline = '') AS without_tagline
            FROM movies;
        """)
        with_tagline, without_tagline = cur.fetchone()

    print("\n📊 Movie tagline summary:")
    print(f"  With tagline: {with_tagline}")
    print(f"  Without tagline: {without_tagline}")

def main():
    conn = psycopg2.connect(
        dbname="",
        user="",
        password="",
        host="",
        port=5432,
    )

    backfill_movie_taglines(conn, TMDB_API_KEY)
    diagnostic_summary(conn)

    conn.close()

if __name__ == "__main__":
    main()
