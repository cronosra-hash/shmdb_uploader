import requests  # Import the requests library to make HTTP requests
from config.settings import TMDB_API_KEY  # Import your TMDB API key from the settings file

def fetch_series(series_id):
    """
    Fetch detailed information about a TV series from TMDB,
    including aggregate credits (with episode counts) and IMDb ID.
    
    Args:
        series_id (int or str): The TMDB ID of the TV series.
    
    Returns:
        dict: A dictionary containing series details, aggregate credits, and IMDb ID.
    """
    base_url = f"https://api.themoviedb.org/3/tv/{series_id}"

    # Fetch main series data (without credits here)
    params = {"api_key": TMDB_API_KEY}
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    series_data = response.json()

    # Fetch aggregate credits (includes total_episode_count)
    credits_url = f"{base_url}/aggregate_credits"
    credits_response = requests.get(credits_url, params={"api_key": TMDB_API_KEY})
    credits_response.raise_for_status()
    series_data["aggregate_credits"] = credits_response.json()

    # Fetch external IDs (IMDb ID, etc.)
    ext_url = f"{base_url}/external_ids"
    ext_response = requests.get(ext_url, params={"api_key": TMDB_API_KEY})
    ext_response.raise_for_status()
    external_ids = ext_response.json()
    series_data["imdb_id"] = external_ids.get("imdb_id") or None

    return series_data


def fetch_season(series_id, season_number, retries=2, timeout=10):
    """
    Fetch detailed information about a specific season of a TV series from TMDB.
    
    Args:
        series_id (int or str): The TMDB ID of the TV series.
        season_number (int): The season number to fetch.
        retries (int): Number of retry attempts on failure.
        timeout (int): Timeout in seconds for the request.
    
    Returns:
        dict: A dictionary containing season details.
    
    Raises:
        requests.HTTPError: If the request fails after retries.
    """
    url = f"https://api.themoviedb.org/3/tv/{series_id}/season/{season_number}"
    params = {"api_key": TMDB_API_KEY}

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            season_data = response.json()
            print(f"✅ Season {season_number} fetched for series_id={series_id} with {len(season_data.get('episodes', []))} episodes")
            return season_data

        except requests.HTTPError as e:
            print(f"❌ HTTP error on season {season_number} (attempt {attempt}): {e}")
            if attempt == retries:
                raise
        except requests.RequestException as e:
            print(f"⚠️ Request failed on season {season_number} (attempt {attempt}): {e}")
            if attempt == retries:
                raise

    # Should never reach here due to raise in final attempt
    return {}


def fetch_all_episodes(series_id):
    """
    Fetch all episodes across all seasons for a given TV series.
    
    Args:
        series_id (int or str): The TMDB ID of the TV series.
    
    Returns:
        list: A list of episode dictionaries with season and series context.
    """
    series_data = fetch_series(series_id)
    all_episodes = []
    seen = set()

    for season in series_data.get("seasons", []):
        season_number = season.get("season_number")
        season_id = season.get("id")

        if season_number is None or season_id is None:
            print(f"⚠️ Skipping season with missing number or ID: {season}")
            continue

        try:
            season_data = fetch_season(series_id, season_number)
            episodes = season_data.get("episodes")
            if not episodes:
                print(f"⚠️ No episodes found for season {season_number}")
                continue

            print(f"✅ Season {season_number} fetched with {len(episodes)} episodes")

            for ep in episodes:
                key = (season_id, ep.get("episode_number"))
                if key in seen:
                    print(f"⚠️ Duplicate episode detected: season_id={season_id}, episode_number={ep.get('episode_number')}")
                    continue
                seen.add(key)

                ep["season_id"] = season_id
                ep["season_number"] = season_number
                ep["series_id"] = series_id
                all_episodes.append(ep)

        except requests.HTTPError as e:
            print(f"❌ Failed to fetch season {season_number}: {e}")
            continue

    print(f"📦 Total episodes fetched: {len(all_episodes)}")
    return all_episodes
