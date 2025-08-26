# diagnose_db.py
import sys
from datetime import datetime, timedelta
from db.connection import get_connection

def classify_freshness(lastupdated):
    if not lastupdated:
        return "stale"
    delta = datetime.utcnow() - lastupdated
    if delta <= timedelta(days=7):
        return "fresh"
    elif delta <= timedelta(days=30):
        return "moderate"
    else:
        return "stale"

def run_diagnostics():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            print("🔍 Running diagnostics...\n")

            # Row counts
            cur.execute("SELECT COUNT(*) FROM movies;")
            movie_count = cur.fetchone()[0]
            print(f"🎬 Movies: {movie_count}")

            cur.execute("SELECT COUNT(*) FROM series;")
            series_count = cur.fetchone()[0]
            print(f"📺 Series: {series_count}")

            # Freshness
            cur.execute("SELECT lastupdated FROM movies UNION ALL SELECT lastupdated FROM series;")
            freshness = {"fresh": 0, "moderate": 0, "stale": 0}
            for row in cur.fetchall():
                status = classify_freshness(row[0])
                freshness[status] += 1
            print(f"\n🧪 Freshness Breakdown:")
            for k, v in freshness.items():
                print(f"  - {k.capitalize()}: {v}")

            # Missing key fields
            cur.execute("SELECT COUNT(*) FROM movies WHERE overview IS NULL OR release_date IS NULL;")
            missing_movies = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM series WHERE overview IS NULL OR first_air_date IS NULL;")
            missing_series = cur.fetchone()[0]
            print(f"\n🚫 Missing Fields:")
            print(f"  - Movies missing overview/release_date: {missing_movies}")
            print(f"  - Series missing overview/first_air_date: {missing_series}")

            # Orphaned logs
            cur.execute("""
                SELECT COUNT(*) FROM update_logs
                WHERE movie_id NOT IN (
                    SELECT id FROM movies
                    UNION
                    SELECT series_id FROM series
                );
            """)
            orphaned = cur.fetchone()[0]
            print(f"\n🕳️ Orphaned update_logs: {orphaned}")

            # Top changed fields
            cur.execute("""
                SELECT field_name, COUNT(*) AS freq
                FROM update_logs
                GROUP BY field_name
                ORDER BY freq DESC
                LIMIT 5;
            """)
            top_fields = cur.fetchall()
            print(f"\n📈 Most Changed Fields:")
            for field, freq in top_fields:
                print(f"  - {field}: {freq} changes")

            # Most updated title
            cur.execute("""
                SELECT movie_id, COUNT(*) AS changes
                FROM update_logs
                GROUP BY movie_id
                ORDER BY changes DESC
                LIMIT 1;
            """)
            row = cur.fetchone()
            print(f"\n🏆 Most Updated Title ID: {row[0]} ({row[1]} changes)")

    except Exception as e:
        print(f"❌ Error during diagnostics: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_diagnostics()
