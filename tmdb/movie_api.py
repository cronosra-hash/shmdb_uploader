import requests  # Import the requests library to make HTTP requests
from config.settings import TMDB_API_KEY  # Import your TMDB API key from the configuration file

def get_movie_data(movie_id):
    """
    Fetch detailed information about a movie from TMDB, including credits.

    Args:
        movie_id (int or str): The TMDB ID of the movie.

    Returns:
        dict or None: A dictionary containing movie details and credits, or None if the movie is not found.
    """

    # Construct the URL for the movie endpoint
    url = f"https://api.themoviedb.org/3/movie/{movie_id}"

    # Define query parameters including API key and request to append credits data
    params = {"api_key": TMDB_API_KEY, "append_to_response": "credits"}

    # Make the request to fetch movie data
    response = requests.get(url, params=params)

    # Handle case where the movie ID is not found (404 error)
    if response.status_code == 404:
        print(f"❌ Movie ID {movie_id} not found (404). Skipping.")
        return None

    # Raise an exception for other HTTP errors (e.g., 500, 403)
    response.raise_for_status()

    # Return the parsed JSON response containing movie details and credits
    return response.json()