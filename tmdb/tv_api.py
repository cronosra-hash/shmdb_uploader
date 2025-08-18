import requests
from config.settings import TMDB_API_KEY

def fetch_series(series_id):
    url = f"https://api.themoviedb.org/3/tv/{series_id}"
    params = {"api_key": TMDB_API_KEY, "append_to_response": "credits"}
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def fetch_season(series_id, season_number):
    url = f"https://api.themoviedb.org/3/tv/{series_id}/season/{season_number}"
    params = {"api_key": TMDB_API_KEY}
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()
