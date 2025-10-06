import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re

# Placeholder for your TMDb API key
TMDB_API_KEY = "02e87018a4bae1782f57cb6e119c3d09"

# Headers to mimic a browser visit
HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# Function to get title and type from TMDb using TMDb ID
def get_title_and_type(tmdb_id):
    base_url = "https://api.themoviedb.org/3/"
    for media_type in ["movie", "tv"]:
        url = f"{base_url}{media_type}/{tmdb_id}?api_key={TMDB_API_KEY}"
        try:
            response = requests.get(url, headers=HEADERS)
            if response.status_code == 200:
                data = response.json()
                title = data.get("title") if media_type == "movie" else data.get("name")
                return title, media_type
        except Exception as e:
            return None, None
    return None, None

# Function to search Metacritic and get the correct review URL
def search_metacritic(title, media_type):
    search_url = f"https://www.metacritic.com/search/{media_type}/{title}/results"
    try:
        response = requests.get(search_url, headers=HEADERS)
        if response.status_code != 200:
            return None
        soup = BeautifulSoup(response.text, "html.parser")
        result = soup.find("li", class_="result first_result")
        if not result:
            result = soup.find("li", class_="result")
        if result:
            link = result.find("a", href=True)
            if link:
                return "https://www.metacritic.com" + link["href"] + "/critic-reviews"
    except Exception:
        return None
    return None

# Function to scrape critic reviews from Metacritic
def scrape_reviews(title, media_type, review_url):
    reviews = []
    try:
        response = requests.get(review_url, headers=HEADERS)
        if response.status_code != 200:
            return [{
                "title": title,
                "type": media_type,
                "critic": "",
                "score": "",
                "publication": "",
                "review_date": "",
                "excerpt": "",
                "review_link": review_url,
                "error": f"HTTP {response.status_code}"
            }]
        soup = BeautifulSoup(response.text, "html.parser")
        review_blocks = soup.find_all("div", class_="review pad_top1 pad_btm1")
        if not review_blocks:
            return [{
                "title": title,
                "type": media_type,
                "critic": "",
                "score": "",
                "publication": "",
                "review_date": "",
                "excerpt": "",
                "review_link": review_url,
                "error": "No reviews found"
            }]
        for block in review_blocks:
            critic = block.find("span", class_="author")
            score = block.find("div", class_=re.compile("metascore_w.*indiv"))
            publication = block.find("span", class_="source")
            date = block.find("span", class_="date")
            excerpt = block.find("div", class_="review_body")
            reviews.append({
                "title": title,
                "type": media_type,
                "critic": critic.text.strip() if critic else "",
                "score": score.text.strip() if score else "",
                "publication": publication.text.strip() if publication else "",
                "review_date": date.text.strip() if date else "",
                "excerpt": excerpt.text.strip() if excerpt else "",
                "review_link": review_url,
                "error": ""
            })
        return reviews
    except Exception as e:
        return [{
            "title": title,
            "type": media_type,
            "critic": "",
            "score": "",
            "publication": "",
            "review_date": "",
            "excerpt": "",
            "review_link": review_url,
            "error": str(e)
        }]

# List of TMDb IDs to process
tmdb_ids = [
    872585,  # Oppenheimer
    85988,   # Succession
    202411,  # The Bear
    545611,  # Everything Everywhere All At Once
    999999,  # Unknown Title
    888888   # Nonexistent Show
]

# Collect all reviews
all_reviews = []

for tmdb_id in tmdb_ids:
    title, media_type = get_title_and_type(tmdb_id)
    if not title:
        all_reviews.append({
            "title": "",
            "type": "",
            "critic": "",
            "score": "",
            "publication": "",
            "review_date": "",
            "excerpt": "",
            "review_link": "",
            "error": "TMDb ID not found"
        })
        continue
    review_url = search_metacritic(title, media_type)
    if not review_url:
        all_reviews.append({
            "title": title,
            "type": media_type,
            "critic": "",
            "score": "",
            "publication": "",
            "review_date": "",
            "excerpt": "",
            "review_link": "",
            "error": "Metacritic page not found"
        })
        continue
    reviews = scrape_reviews(title, media_type, review_url)
    all_reviews.extend(reviews)
    time.sleep(1)  # Rate limiting

# Save to CSV
df = pd.DataFrame(all_reviews)
df.to_csv("metacritic_reviews.csv", index=False)
