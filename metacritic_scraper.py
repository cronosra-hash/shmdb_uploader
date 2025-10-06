import requests
from bs4 import BeautifulSoup
import csv
import time
import re

def slugify(title):
    slug = title.lower()
    slug = re.sub(r'[^a-z0-9\\s]', '', slug)
    slug = re.sub(r'\\s+', '-', slug.strip())
    return slug

def scrape_critic_reviews(title, content_type):
    slug = slugify(title)
    url = f"https://www.metacritic.com/{content_type}/{slug}/critic-reviews"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            return [{
                "title": title,
                "type": content_type,
                "critic": "",
                "score": "",
                "publication": "",
                "review_date": "",
                "excerpt": "",
                "review_link": "",
                "error": f"HTTP {response.status_code}"
            }]

        soup = BeautifulSoup(response.text, "html.parser")
        reviews = soup.find_all("div", class_="review pad_top1 pad_btm1")

        results = []
        for review in reviews:
            score_tag = review.find("div", class_=re.compile(r"metascore_w large .* indiv"))
            author_tag = review.find("span", class_="author")
            pub_tag = review.find("div", class_="source")
            date_tag = review.find("div", class_="date")
            excerpt_tag = review.find("div", class_="review_body")

            results.append({
                "title": title,
                "type": content_type,
                "critic": author_tag.text.strip() if author_tag else "",
                "score": score_tag.text.strip() if score_tag else "",
                "publication": pub_tag.text.strip() if pub_tag else "",
                "review_date": date_tag.text.strip() if date_tag else "",
                "excerpt": excerpt_tag.text.strip() if excerpt_tag else "",
                "review_link": url,
                "error": ""
            })

        return results if results else [{
            "title": title,
            "type": content_type,
            "critic": "",
            "score": "",
            "publication": "",
            "review_date": "",
            "excerpt": "",
            "review_link": url,
            "error": "No reviews found"
        }]

    except Exception as e:
        return [{
            "title": title,
            "type": content_type,
            "critic": "",
            "score": "",
            "publication": "",
            "review_date": "",
            "excerpt": "",
            "review_link": url,
            "error": str(e)
        }]

def main():
    titles = [
        {"title": "Oppenheimer", "type": "movie"},
        {"title": "Succession", "type": "tv"},
        {"title": "The Bear", "type": "tv"},
        {"title": "Everything Everywhere All At Once", "type": "movie"},
        {"title": "Unknown Title", "type": "movie"},
        {"title": "Nonexistent Show", "type": "tv"}
    ]

    all_reviews = []
    for item in titles:
        reviews = scrape_critic_reviews(item["title"], item["type"])
        all_reviews.extend(reviews)
        time.sleep(2)  # Rate limiting

    with open("metacritic_reviews.csv", mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=[
            "title", "type", "critic", "score", "publication",
            "review_date", "excerpt", "review_link", "error"
        ])
        writer.writeheader()
        writer.writerows(all_reviews)

    print("Scraping completed. Results saved to metacritic_reviews.csv.")

if __name__ == "__main__":
    main()
