import requests
from config.settings import TMDB_API_KEY

def fetch_series(series_id):
    base_url = f"https://api.themoviedb.org/3/tv/{series_id}"
    params = {"api_key": TMDB_API_KEY, "append_to_response": "credits"}
    
    # Fetch main series data
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    series_data = response.json()

    # Fetch external IDs (for imdb_id)
    ext_url = f"{base_url}/external_ids"
    ext_response = requests.get(ext_url, params={"api_key": TMDB_API_KEY})
    ext_response.raise_for_status()
    external_ids = ext_response.json()

    # Inject imdb_id into series_data
    series_data["imdb_id"] = external_ids.get("imdb_id") or None

    return series_data


def fetch_season(series_id, season_number):
    url = f"https://api.themoviedb.org/3/tv/{series_id}/season/{season_number}"
    params = {"api_key": TMDB_API_KEY}
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()
