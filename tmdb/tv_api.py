import requests  # Import the requests library to make HTTP requests
from config.settings import TMDB_API_KEY  # Import your TMDB API key from the settings file

def fetch_series(series_id):
    """
    Fetch detailed information about a TV series from TMDB, including credits and IMDb ID.
    
    Args:
        series_id (int or str): The TMDB ID of the TV series.
    
    Returns:
        dict: A dictionary containing series details, credits, and IMDb ID.
    """
    
    # Construct the base URL for the TV series endpoint
    base_url = f"https://api.themoviedb.org/3/tv/{series_id}"
    
    # Define query parameters including API key and request to append credits data
    params = {"api_key": TMDB_API_KEY, "append_to_response": "credits"}
    
    # Make the request to fetch main series data including credits
    response = requests.get(base_url, params=params)
    response.raise_for_status()  # Raise an error if the request fails
    series_data = response.json()  # Parse the JSON response into a Python dictionary

    # Construct the URL to fetch external IDs (e.g., IMDb ID)
    ext_url = f"{base_url}/external_ids"
    
    # Make the request to fetch external IDs
    ext_response = requests.get(ext_url, params={"api_key": TMDB_API_KEY})
    ext_response.raise_for_status()  # Raise an error if the request fails
    external_ids = ext_response.json()  # Parse the JSON response

    # Add the IMDb ID to the series data dictionary, or None if not available
    series_data["imdb_id"] = external_ids.get("imdb_id") or None

    # Return the complete series data including credits and IMDb ID
    return series_data


def fetch_season(series_id, season_number):
    """
    Fetch detailed information about a specific season of a TV series from TMDB.
    
    Args:
        series_id (int or str): The TMDB ID of the TV series.
        season_number (int): The season number to fetch.
    
    Returns:
        dict: A dictionary containing season details.
    """
    
    # Construct the URL for the specific season of the series
    url = f"https://api.themoviedb.org/3/tv/{series_id}/season/{season_number}"
    
    # Define query parameters with the API key
    params = {"api_key": TMDB_API_KEY}
    
    # Make the request to fetch season data
    response = requests.get(url, params=params)
    response.raise_for_status()  # Raise an error if the request fails
    
    # Return the parsed JSON response containing season details
    return response.json()
