# tmdb/people_api.py

import requests
from datetime import datetime
from config.settings import TMDB_API_KEY
from db.helpers import dict_cursor


def search_person_tmdb(name: str):
    url = "https://api.themoviedb.org/3/search/person"
    params = {"api_key": TMDB_API_KEY, "query": name}
    response = requests.get(url, params=params)
    if response.status_code != 200:
        return []

    people = response.json().get("results", [])

    with dict_cursor() as cur:
        for person in people:
            person_id = person.get("id")

            # Fetch detailed info
            detail_url = f"https://api.themoviedb.org/3/person/{person_id}"
            detail_response = requests.get(detail_url, params={"api_key": TMDB_API_KEY})
            if detail_response.status_code == 200:
                details = detail_response.json()
                person["biography"] = details.get("biography")
                person["birthday"] = details.get("birthday")
                person["place_of_birth"] = details.get("place_of_birth")
                person["also_known_as"] = details.get("also_known_as")

            # Fetch credits
            credits_url = f"https://api.themoviedb.org/3/person/{person_id}/combined_credits"
            credits_response = requests.get(credits_url, params={"api_key": TMDB_API_KEY})
            if credits_response.status_code == 200:
                credits = credits_response.json().get("cast", [])
                for credit in credits:
                    tmdb_id = credit.get("id")
                    media_type = credit.get("media_type")
                    date_str = (
                        credit.get("release_date")
                        if media_type == "movie"
                        else credit.get("first_air_date")
                    )

                    # Extract release year
                    credit["release_year"] = date_str[:4] if date_str else None

                    # Check existence in DB
                    if media_type == "movie":
                        cur.execute("SELECT 1 FROM movies WHERE movie_id = %s;", (tmdb_id,))
                    elif media_type == "tv":
                        cur.execute("SELECT 1 FROM series WHERE series_id = %s;", (tmdb_id,))
                    else:
                        credit["exists"] = False
                        credit["sort_date"] = None
                        continue

                    credit["exists"] = cur.fetchone() is not None

                    # Enrich with IMDb ID
                    if media_type == "movie":
                        detail_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
                        resp = requests.get(detail_url, params={"api_key": TMDB_API_KEY})
                        if resp.status_code == 200:
                            credit["imdb_id"] = resp.json().get("imdb_id")
                    elif media_type == "tv":
                        external_url = f"https://api.themoviedb.org/3/tv/{tmdb_id}/external_ids"
                        resp = requests.get(external_url, params={"api_key": TMDB_API_KEY})
                        if resp.status_code == 200:
                            credit["imdb_id"] = resp.json().get("imdb_id")

                    # Parse date for