import os
import psycopg2
import requests
from datetime import datetime
import json

TMDB_API_KEY = "02e87018a4bae1782f57cb6e119c3d09"


def fetch_series_credits(series_id, tmdb_api_key):
    """
    Fetch aggregate credits for a TV series from TMDb.
    This endpoint includes total_episode_count and guest stars.
    """
    url = f"https://api.themoviedb.org/3/tv/{series_id}/aggregate_credits"
    params = {"api_key": tmdb_api_key}
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    return resp.json()

def ensure_person_exists(cur, cast):
    """
    Ensure the person exists in your people table.
    Adjust fields if your schema has more columns.
    """
    cur.execute(
        "INSERT INTO people (person_id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
        (cast["id"], cast["name"])
    )


def backfill_cast_data(conn, tmdb_api_key):
    with conn.cursor() as cur:
        cur.execute("SELECT series_id, series_name FROM series;")
        series_rows = cur.fetchall()

    for series_id, series_name in series_rows:
        print(f"🔄 Updating cast for {series_name} (ID={series_id})")

        try:
            credits = fetch_series_credits(series_id, tmdb_api_key)
            cast_list = credits.get("cast", [])

            with conn.cursor() as cur:
                for cast in cast_list:
                    ensure_person_exists(cur, cast)

                    episode_count = cast.get("total_episode_count", 1)
                    character_name = None
                    if cast.get("roles"):
                        character_name = cast["roles"][0].get("character")

                    cur.execute(
                        """
                        INSERT INTO series_cast (
                            series_id, person_id, cast_order, character_name, episode_count, last_updated
                        )
                        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (series_id, person_id) DO UPDATE
                        SET episode_count = EXCLUDED.episode_count,
                            character_name = COALESCE(EXCLUDED.character_name, series_cast.character_name),
                            last_updated = CURRENT_TIMESTAMP;
                        """,
                        (
                            series_id,
                            cast["id"],
                            cast.get("order", 0),
                            character_name,
                            episode_count,
                        ),
                    )

            conn.commit()
            print(f"✅ Cast updated for {series_name}")

        except Exception as e:
            print(f"❌ Failed to update {series_name}: {e}")
            conn.rollback()

if __name__ == "__main__":
    conn = psycopg2.connect(
        dbname="shmdb",
        user="neondb_owner",
        password="npg_YF0meUgAXDB5",
        host="ep-raspy-truth-abxbigmv-pooler.eu-west-2.aws.neon.tech",
        port=5432,
    )
    backfill_cast_data(conn, TMDB_API_KEY)
    conn.close()

