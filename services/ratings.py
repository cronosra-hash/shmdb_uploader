import requests
import pandas as pd
import time

TMDB_API_KEY = '02e87018a4bae1782f57cb6e119c3d09'
OMDB_API_KEY = '16894f6c'

def get_movie_details(movie_id):
    url = f'https://api.themoviedb.org/3/movie/{movie_id}'
    params = {'api_key': TMDB_API_KEY}
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"Failed to fetch movie ID {movie_id}")
        return None
    return response.json()

def get_tv_details(tv_id):
    url = f'https://api.themoviedb.org/3/tv/{tv_id}'
    params = {'api_key': TMDB_API_KEY}
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"Failed to fetch TV ID {tv_id}")
        return None
    return response.json()

def get_tv_imdb_id(tv_id):
    url = f'https://api.themoviedb.org/3/tv/{tv_id}/external_ids'
    params = {'api_key': TMDB_API_KEY}
    response = requests.get(url, params=params)
    if response.status_code != 200:
        return None
    return response.json().get('imdb_id')

def get_omdb_ratings(imdb_id):
    if not imdb_id:
        return None, None, None
    url = f"http://www.omdbapi.com/?i={imdb_id}&apikey={OMDB_API_KEY}"
    response = requests.get(url)
    if response.status_code != 200:
        return None, None, None
    data = response.json()

    # IMDb rating (already out of 10)
    try:
        imdb_rating = float(data.get('imdbRating')) if data.get('imdbRating') else None
    except ValueError:
        imdb_rating = None

    rt_rating = None
    mc_rating = None

    for rating in data.get('Ratings', []):
        source = rating['Source']
        value = rating['Value']

        if source == 'Rotten Tomatoes':
            try:
                rt_rating = float(value.strip('%')) / 10
            except ValueError:
                rt_rating = None

        elif source == 'Metacritic':
            try:
                mc_rating = int(value.split('/')[0]) / 10
            except (ValueError, IndexError):
                mc_rating = None

    return imdb_rating, rt_rating, mc_rating

def process_titles(titles, output_file='ratings.xlsx'):
    movie_data = []
    series_data = []

    for title in titles:
        title_id = title['id']
        title_type = title['type']  # 'movie' or 'tv'

        if title_type == 'movie':
            details = get_movie_details(title_id)
            imdb_id = details.get('imdb_id') if details else None
        elif title_type == 'tv':
            details = get_tv_details(title_id)
            imdb_id = get_tv_imdb_id(title_id) if details else None
        else:
            continue

        if not details:
            continue

        imdb_rating, rt_rating, mc_rating = get_omdb_ratings(imdb_id)
        record = {
            'title_type': title_type,
            'title_id': title_id,
            'imdb_id': imdb_id,
            'title_name': details.get('name') or details.get('title'),
            'imdb_rating': imdb_rating,
            'rotten_tomatoes_rating': rt_rating,
            'metacritic_rating': mc_rating
        }

        if title_type == 'movie':
            movie_data.append(record)
        else:
            series_data.append(record)

        time.sleep(0.25)

    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        pd.DataFrame(movie_data).to_excel(writer, sheet_name='Movies', index=False)
        pd.DataFrame(series_data).to_excel(writer, sheet_name='Series', index=False)

    print(f"Saved ratings to {output_file}")

# Example usage
titles = [
    {'id': 7451, 'type': 'movie'},     # Example movie
    {'id': 1399, 'type': 'tv'},        # Game of Thrones
    {'id': 60574, 'type': 'tv'},       # Peaky Blinders
]
process_titles(titles)
