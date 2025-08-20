from db.connection import get_connection
from tmdb.tv_api import fetch_series
from uploader.tv_uploader import insert_or_update_series_data

SERIES_IDS = [39435, 90669]

def main():
    conn = get_connection()
    try:
        for sid in SERIES_IDS:
            print(f"🔍 Fetching Series ID {sid}")
            try:
                series = fetch_series(sid)
                insert_or_update_series_data(conn, series)
            except Exception as e:
                print(f"❌ Failed to process {sid}: {e}")
                conn.rollback()
    finally:
        conn.close()
        print("✅ Sync complete. Connection closed.")

if __name__ == "__main__":
    main()
