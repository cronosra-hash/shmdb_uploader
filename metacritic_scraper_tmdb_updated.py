import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

# Replace with your actual TMDb API key
TMDB_API_KEY = "02e87018a4bae1782f57cb6e119c3d09"

# Sample TMDb ID list (can be replaced with actual input)
tmdb_ids = [872585, 1399, 202411, 545611]  # Oppenheimer, Game of Thrones, The Bear, Everything Everywhere All At Once

# Headers to mimic a browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# Function to get title and type from TMDb
def get_title_and_type(tmdb_id):
    for media_type in ['movie', 'tv']:
        url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}?api_key={TMDB_API_KEY}"
        try:
            response = requests.get(url, headers=HEADERS)
            if response.status_code == 200:
                data = response.json()
                title = data.get('title') or data.get('name')
                return title, media_type
        except Exception as e:
            return None, None
    return None, None

# Function to search Metacritic and get the correct URL
def search_metacritic(title, media_type):
    search_url = f"https://www.metacritic.com/search/{media_type}/{requests.utils.quote(title)}/results"
    try:
        response = requests.get(search_url, headers=HEADERS)
        if response.status_code != 200:
            return None
        soup = BeautifulSoup(response.text, 'html.parser')
        result = soup.select_one("li.result.first_result a")
        if result and result.get('href'):
            return "https://www.metacritic.com" + result['href'] + "/critic-reviews"
    except Exception:
        return None
    return None

# Function to scrape critic reviews
def scrape_reviews(url):
    reviews = []
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            return [], f"HTTP {response.status_code}"
        soup = BeautifulSoup(response.text, 'html.parser')
        review_blocks = soup.select("div.review.critic_review")
        if not review_blocks:
            return [], "No reviews found"
        for block in review_blocks:
            critic = block.select_one(".author")
            score = block.select_one(".metascore_w")
            publication = block.select_one(".source")
            date = block.select_one(".date")
            excerpt = block.select_one(".review_body")
            reviews.append({
                "critic": critic.get_text(strip=True) if critic else "",
                "score": score.get_text(strip=True) if score else "",
                "publication": publication.get_text(strip=True) if publication else "",
                "review_date": date.get_text(strip=True) if date else "",
                "excerpt": excerpt.get_text(strip=True) if excerpt else "",
                "review_link": url,
                "error": ""
            })
        return reviews, ""
    except Exception as e:
        return [], str(e)

# Main logic
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
            "error": "TMDb lookup failed"
        })
        continue

    url = search_metacritic(title, media_type)
    if not url:
        all_reviews.append({
            "title": title,
            "type": media_type,
            "critic": "",
            "score": "",
            "publication": "",
            "review_date": "",
            "excerpt": "",
            "review_link": "",
            "error": "Metacritic search failed"
        })
        continue

    reviews, error = scrape_reviews(url)
    if reviews:
        for review in reviews:
            review.update({"title": title, "type": media_type})
            all_reviews.append(review)
    else:
        all_reviews.append({
            "title": title,
            "type": media_type,
            "critic": "",
            "score": "",
            "publication": "",
            "review_date": "",
            "excerpt": "",
            "review_link": url,
            "error": error
        })

    time.sleep(2)  # Rate limiting

# Save to CSV
df = pd.DataFrame(all_reviews)
df.to_csv("metacritic_reviews.csv", index=False)
