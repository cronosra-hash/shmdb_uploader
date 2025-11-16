#!/usr/bin/env python3
"""
Backfill TMDb crew data into your database.
- Cleans up duplicate 'Unknown' jobs where a valid job exists.
- Fetches aggregate credits for each series.
- Inserts crew with proper jobs from crew["jobs"] array.
"""

import os
import psycopg2
import requests
from datetime import datetime
import json

TMDB_API_KEY = "02e87018a4bae1782f57cb6e119c3d09"

def fetch_series_aggregate_credits(series_id, tmdb_api_key):
    """Fetch aggregate credits for a TV series from TMDb."""
    url = f"https://api.themoviedb.org/3/tv/{series_id}/aggregate_credits"
    params = {"api_key": tmdb_api_key}
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    return resp.json()

def ensure_person_exists(cur, person):
    """Ensure the person exists in the people table."""
    cur.execute(
        "INSERT INTO people (person_id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
        (person["id"], person["name"])
    )

def cleanup_unknowns(conn):
    """Remove 'Unknown' jobs where a valid job exists for the same person/series."""
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM series_crew sc_unknown
            USING series_crew sc_real
            WHERE sc_unknown.series_id = sc_real.series_id
              AND sc_unknown.person_id = sc_real.person_id
              AND sc_unknown.job = 'Unknown'
              AND sc_real.job <> 'Unknown';
        """)
    conn.commit()
    print("🧹 Cleaned up duplicate 'Unknown' crew rows.")

def backfill_series_crew(conn, tmdb_api_key):
    """Backfill crew data for all series."""
    with conn.cursor() as cur:
        cur.execute("SELECT series_id, series_name FROM series;")
        series_rows = cur.fetchall()

    for series_id, series_name in series_rows:
        print(f"🔄 Updating crew for {series_name} (ID={series_id})")

        try:
            credits = fetch_series_aggregate_credits(series_id, tmdb_api_key)
            crew_list = credits.get("crew", [])

            with conn.cursor() as cur:
                for crew in crew_list:
                    ensure_person_exists(cur, crew)

                    department = crew.get("department")
                    jobs = [j.get("job") for j in crew.get("jobs", []) if j.get("job")]
                    if not jobs:
                        jobs = ["Unknown"]

                    for job in jobs:
                        cur.execute(
                            """
                            INSERT INTO series_crew (
                                series_id, person_id, department, job, last_updated
                            )
                            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                            ON CONFLICT (series_id, person_id, job) DO UPDATE
                            SET department = COALESCE(EXCLUDED.department, series_crew.department),
                                last_updated = CURRENT_TIMESTAMP;
                            """,
                            (series_id, crew["id"], department, job),
                        )

            conn.commit()
            print(f"✅ Crew updated for {series_name}")

        except Exception as e:
            print(f"❌ Failed to update {series_name}: {e}")
            conn.rollback()

def main():
    conn = psycopg2.connect(
        dbname="shmdb",
        user="neondb_owner",
        password="npg_YF0meUgAXDB5",
        host="ep-raspy-truth-abxbigmv-pooler.eu-west-2.aws.neon.tech",
        port=5432,
    )

    cleanup_unknowns(conn)
    backfill_series_crew(conn, TMDB_API_KEY)
    diagnostic_summary(conn)

    conn.close()


if __name__ == "__main__":
    main()

def diagnostic_summary(conn):
    """Print a summary of crew roles per series."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT s.series_name,
                   sc.job,
                   COUNT(*) AS crew_count
            FROM series_crew sc
            JOIN series s ON s.series_id = sc.series_id
            GROUP BY s.series_name, sc.job
            ORDER BY s.series_name, sc.job;
        """)
        rows = cur.fetchall()

    print("\n📊 Crew role summary:")
    current_series = None
    for series_name, job, count in rows:
        if series_name != current_series:
            print(f"\n{series_name}")
            current_series = series_name
        print(f"  {job}: {count}")
