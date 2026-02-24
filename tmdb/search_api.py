# tmdb/search_api.py
import requests
from config.settings import TMDB_API_KEY
from datetime import datetime

def search_tmdb_combined(name):
    endpoints = ["movie", "tv"]
    combined_results = []

    for media_type in endpoints:
        url = f"https://api.themoviedb.org/3/search/{media_type}"
        params = {"api_key": TMDB_API_KEY, "query": name, "include_adult": False}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            results = response.json().get("results", [])
            for result in results:
                result["media_type"] = media_type

                # Enrich with imdb_id
                detail_url = (
                    f"https://api.themoviedb.org/3/movie/{result['id']}"
                    if media_type == "movie"
                    else f"https://api.themoviedb.org/3/tv/{result['id']}/external_ids"
                )
                detail_response = requests.get(
                    detail_url, params={"api_key": TMDB_API_KEY}
                )
                if detail_response.status_code == 200:
                    detail_data = detail_response.json()
                    result["imdb_id"] = detail_data.get("imdb_id")

                combined_results.append(result)

    combined_results.sort(key=lambda x: x.get("popularity", 0), reverse=True)
    return combined_results

def get_tmdb_data(tmdb_id, media_type):
    """
    Generic TMDb fetcher for movies and TV series.
    """
    if media_type not in ["movie", "tv"]:
        raise ValueError(f"Unsupported media type: {media_type}")

    url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}"
    params = {"api_key": TMDB_API_KEY, "language": "en-US"}

    response = requests.get(url, params=params)

    if response.ok:
        return response.json()

    print(f"❌ TMDb fetch failed for ID {tmdb_id}: {response.status_code}")
    return {}
