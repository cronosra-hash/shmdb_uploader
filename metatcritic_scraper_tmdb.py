import requests
from bs4 import BeautifulSoup
import csv
import time
import re

# Placeholder for your TMDb API key
TMDB_API_KEY = "YOUR_TMDB_API_KEY"

# Sample list of TMDb IDs
tmdb_ids = [
    872585,  # Oppenheimer (movie)
    1399,    # Game of Thrones (tv)
    114472,  # The Bear (tv)
    545611   # Everything Everywhere All At Once (movie)
]

# Headers to mimic a browser
HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# Function to get title and type from TMDb
def get_title_and_type(tmdb_id):
    for media_type in ['movie', 'tv']:
        url = f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}?api_key={TMDB_API_KEY}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            title = data.get('title') if media_type == 'movie' else data.get('name')
            return title, media_type
    return None, None

# Function to convert title to Metacritic slug
def title_to_slug(title):
    slug = title.lower()
    slug = re.sub(r'[^\w\s-]', '', slug)
    slug = re.sub(r'\s+', '-', slug)
    return slug

# Function to scrape Metacritic critic reviews
def scrape_metacritic_reviews(title, media_type):
    slug = title_to_slug(title)
    base_url = f"https://www.metacritic.com/{media_type}/{slug}/critic-reviews"
    reviews = []

    try:
        response = requests.get(base_url, headers=HEADERS)
        if response.status_code != 200:
            return [{"title": title, "type": media_type, "critic": "", "score": "", "publication": "", "review_date": "", "excerpt": "", "review_link": "", "error": "Page not found"}]

        soup = BeautifulSoup(response.text, 'html.parser')
        review_blocks = soup.find_all('div', class_='review pad_top1 pad_btm1')

        for block in review_blocks:
            score_tag = block.find('div', class_='metascore_w')
            critic_tag = block.find('span', class_='author')
            pub_tag = block.find('div', class_='source')
            date_tag = block.find('div', class_='date')
            excerpt_tag = block.find('div', class_='review_body')
            link_tag = block.find('a', href=True)

            score = score_tag.text.strip() if score_tag else ""
            critic = critic_tag.text.strip() if critic_tag else ""
            publication = pub_tag.text.strip() if pub_tag else ""
            review_date = date_tag.text.strip() if date_tag else ""
            excerpt = excerpt_tag.text.strip() if excerpt_tag else ""
            review_link = f"https://www.metacritic.com{link_tag['href']}" if link_tag else ""

            reviews.append({
                "title": title,
                "type": media_type,
                "critic": critic,
                "score": score,
                "publication": publication,
                "review_date": review_date,
                "excerpt": excerpt,
                "review_link": review_link,
                "error": ""
            })

        time.sleep(1)  # Rate limiting
        return reviews

    except Exception as e:
        return [{"title": title, "type": media_type, "critic": "", "score": "", "publication": "", "review_date": "", "excerpt": "", "review_link": "", "error": str(e)}]

# Main script
all_reviews = []

for tmdb_id in tmdb_ids:
    title, media_type = get_title_and_type(tmdb_id)
    if title and media_type:
        reviews = scrape_metacritic_reviews(title, media_type)
        all_reviews.extend(reviews)
    else:
        all_reviews.append({
            "title": "",
            "type": "",
            "critic": "",
            "score": "",
            "publication": "",
            "review_date": "",
            "excerpt": "",
            "review_link": "",
            "error": f"TMDb ID {tmdb_id} not found"
        })

# Write to CSV
csv_file = "metacritic_reviews.csv"
with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=["title", "type", "critic", "score", "publication", "review_date", "excerpt", "review_link", "error"])
    writer.writeheader()
    for review in all_reviews:
        writer.writerow(review)

print(f"Scraping completed. Results saved to {csv_file}")