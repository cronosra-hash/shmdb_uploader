# uploader/media_processor.py

from config.settings import TMDB_API_KEY
from tmdb.movie_api import get_movie_data
from tmdb.tv_api import fetch_series, fetch_all_episodes
from uploader.movie_uploader import insert_or_update_movie_data
from uploader.tv_uploader import (
    insert_or_update_series_data,
    sync_series_seasons,
    sync_series_episodes,
)


def process_media_upload(conn, tmdb_id, media_type):
    """
    Handles uploading/syncing of movie or TV series data.
    Uses an existing DB connection (shared across bulk uploads).
    """

    if media_type == "movie":
        movie_data = get_movie_data(tmdb_id)
        insert_or_update_movie_data(conn, movie_data, media_type)

        print(f"🎬 Movie '{movie_data.get('title')}' synced (id={movie_data['id']})")

        return movie_data["id"], (
            f"✅ Movie '{movie_data.get('title')}' processed successfully."
        )

    elif media_type == "tv":
        series_data = fetch_series(tmdb_id)
        series_id = series_data.get("id")
        series_name = series_data.get("name")

        print(f"📺 Starting sync for TV Series '{series_name}' (id={series_id})")

        # Insert/update series first
        insert_or_update_series_data(conn, series_data, TMDB_API_KEY)

        # Sync seasons
        with conn.cursor() as cur:
            sync_series_seasons(cur, series_data)

        # Fetch episodes
        episodes = fetch_all_episodes(series_id)
        if not episodes:
            print(f"⚠️ No episodes found for series_id={series_id}")
        else:
            print(f"📦 {len(episodes)} episodes fetched for series_id={series_id}")

        series_data["episodes"] = episodes

        # Sync episodes
        with conn.cursor() as cur:
            sync_series_episodes(cur, series_id, series_data)

        conn.commit()

        print(f"✅ TV Series '{series_name}' synced successfully")

        return series_id, (
            f"✅ TV Series '{series_name}' processed successfully."
        )

    print(f"❌ Unsupported media type: {media_type}")
    return None, "❌ Invalid media type selected."
