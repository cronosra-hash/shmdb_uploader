import requests
from config.settings import TMDB_API_KEY

def get_movie_data(movie_id):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}"
    params = {"api_key": TMDB_API_KEY, "append_to_response": "credits"}
    response = requests.get(url, params=params)
    if response.status_code == 404:
        print(f"❌ Movie ID {movie_id} not found (404). Skipping.")
        return None
    response.raise_for_status()
    return response.json()
