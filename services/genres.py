import requests
from typing import Dict
from config.settings import TMDB_API_KEY

TMDB_BASE = "https://api.themoviedb.org/3"

_movie_genres = None
_tv_genres = None


def get_genre_map(content_type: str) -> Dict[int, str]:
    global _movie_genres, _tv_genres

    if content_type == "movie" and _movie_genres:
        return _movie_genres
    if content_type == "tv" and _tv_genres:
        return _tv_genres

    url = f"{TMDB_BASE}/genre/{content_type}/list"
    params = {"api_key": TMDB_API_KEY, "language": "en-GB"}

    response = requests.get(url, params=params)
    genres = response.json().get("genres", [])
    genre_map = {g["id"]: g["name"] for g in genres}

    if content_type == "movie":
        _movie_genres = genre_map
    else:
        _tv_genres = genre_map

    return genre_map
