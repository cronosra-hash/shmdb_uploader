# services/releases.py

import requests
from datetime import datetime, timedelta
from typing import List, Dict
from config.settings import TMDB_API_KEY
from services.genres import get_genre_map

TMDB_BASE = "https://api.themoviedb.org/3"


def get_movie_details(movie_id: int) -> Dict:
    """Fetch full movie details from TMDb."""
    url = f"{TMDB_BASE}/movie/{movie_id}"
    params = {"api_key": TMDB_API_KEY, "language": "en-GB"}

    try:
        response = requests.get(url, params=params, timeout=5)
        response.raise_for_status()
        return response.json()
    except Exception:
        return {}


def get_cinema_releases(month: int = None, year: int = None) -> List[Dict]:
    genre_map = get_genre_map("movie")
    releases = []

    for ym in get_month_range(month, year):
        y, m = map(int, ym.split("-"))

        url = f"{TMDB_BASE}/discover/movie"
        params = {
            "api_key": TMDB_API_KEY,
            "sort_by": "popularity.desc",
            "release_date.gte": f"{y}-{m:02d}-01",
            "release_date.lte": f"{y}-{m:02d}-31",
            "with_original_language": "en",
        }

        response = requests.get(url, params=params)
        movies = response.json().get("results", [])

        for movie in movies:
            release_date = movie.get("release_date", "")

            if isinstance(release_date, str) and release_date.startswith(f"{y}-{m:02d}"):
                details = get_movie_details(movie["id"])

                distributor = ""
                if details.get("production_companies"):
                    distributor = details["production_companies"][0].get("name", "")

                releases.append(
                    {
                        "title": movie["title"],
                        "release_date": (
                            datetime.fromisoformat(release_date)
                            if release_date
                            else None
                        ),
                        "runtime": details.get("runtime", "Unknown"),
                        "certification": details.get("certification", "Unrated"),
                        "distributor": distributor or "Unknown",
                        "genre": " / ".join(
                            genre_map.get(gid, "Unknown")
                            for gid in movie.get("genre_ids", [])
                        ),
                        "poster_path": movie.get("poster_path"),
                        "source": "TMDb",
                        "source_url": f"https://www.themoviedb.org/movie/{movie['id']}",
                    }
                )

    return releases


def get_tv_platform(tv_id: int) -> str:
    url = f"{TMDB_BASE}/tv/{tv_id}"
    params = {"api_key": TMDB_API_KEY, "language": "en-GB"}

    try:
        response = requests.get(url, params=params, timeout=5)
        data = response.json()
        networks = data.get("networks", [])
        return networks[0]["name"] if networks else "Unknown"
    except Exception:
        return "Unknown"


def get_tv_releases(month: int = None, year: int = None) -> List[Dict]:
    genre_map = get_genre_map("tv")
    releases = []

    for ym in get_month_range(month, year):
        y, m = map(int, ym.split("-"))

        url = f"{TMDB_BASE}/discover/tv"
        params = {
            "api_key": TMDB_API_KEY,
            "sort_by": "first_air_date.asc",
            "first_air_date.gte": f"{y}-{m:02d}-01",
            "first_air_date.lte": f"{y}-{m:02d}-31",
            "with_original_language": "en",
        }

        try:
            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            shows = response.json().get("results", [])
        except Exception as e:
            print(f"Error fetching TV releases for {y}-{m:02d}: {e}")
            continue

        for s in shows:
            air_date_str = s.get("first_air_date", "")

            try:
                air_date = (
                    datetime.fromisoformat(air_date_str) if air_date_str else None
                )
            except ValueError:
                air_date = None

            if not air_date or air_date.strftime("%Y-%m") != f"{y}-{m:02d}":
                continue

            # Fetch broadcaster info
            detail_url = f"{TMDB_BASE}/tv/{s['id']}"
            detail_params = {"api_key": TMDB_API_KEY}

            try:
                detail_response = requests.get(
                    detail_url, params=detail_params, timeout=5
                )
                detail_response.raise_for_status()
                detail_data = detail_response.json()
            except Exception as e:
                print(f"Error fetching TV details for {s['id']}: {e}")
                continue

            broadcasters = detail_data.get("networks", [])
            broadcaster_names = [b.get("name") for b in broadcasters if b.get("name")]
            broadcaster = (
                ", ".join(broadcaster_names) if broadcaster_names else "Unknown"
            )

            releases.append(
                {
                    "title": s.get("name", "Untitled"),
                    "release_date": air_date,
                    "platform": broadcaster,
                    "genre": " / ".join(
                        genre_map.get(gid, "Unknown")
                        for gid in s.get("genre_ids", [])
                    ),
                    "poster_path": s.get("poster_path"),
                    "source": "TMDb",
                    "source_url": f"https://www.themoviedb.org/tv/{s['id']}",
                }
            )

    return releases

def get_month_range(month: int = None, year: int = None):
    if month and year:
        return [f"{year}-{month:02d}"]

    now = datetime.now()
    next_month = (now.replace(day=1) + timedelta(days=32)).replace(day=1)

    return [now.strftime("%Y-%m"), next_month.strftime("%Y-%m")]
