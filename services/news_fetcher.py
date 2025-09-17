import feedparser
import requests
from datetime import datetime, timezone
from dateutil import parser
from pydantic import BaseModel
from typing import List, Callable, Optional
from typing import Optional, List, Callable

class NewsArticle(BaseModel):
    title: str
    link: str
    published: datetime
    source: str


def parse_feed(url: str, source: str, limit: int = 5) -> List[NewsArticle]:
    try:
        feed = feedparser.parse(url)
        return [
            NewsArticle(
                title=entry.title,
                link=entry.link,
                published=parser.parse(entry.published),
                source=source,
            )
            for entry in feed.entries[:limit]
        ]
    except Exception as e:
        print(f"{source} fetch error: {e}")
        return []


def fetch_empire_news() -> List[NewsArticle]:
    return parse_feed("https://www.empireonline.com/movies/news/rss/", "Empire Online")


def fetch_screenrant_news() -> List[NewsArticle]:
    return parse_feed("https://screenrant.com/feed/", "Screen Rant")


def fetch_newsdata_io(api_key: Optional[str] = None, limit: int = 5) -> List[NewsArticle]:
    if not api_key:
        print("NewsData.io API key missing")
        return []

    url = f"https://newsdata.io/api/1/news?apikey={api_key}&q=movies&language=en"
    try:
        response = requests.get(url).json()
        raw_results = response.get("results", [])
        if not isinstance(raw_results, list):
            print("Unexpected format from NewsData.io")
            return []

        return [
            NewsArticle(
                title=item.get("title", "Untitled"),
                link=item.get("link", "#"),
                published=parser.parse(item.get("pubDate", datetime.utcnow().isoformat())),
                source="NewsData.io"
            )
            for item in raw_results[:limit]
        ]
    except Exception as e:
        print(f"NewsData fetch error: {e}")
        return []


def get_all_news(api_key: Optional[str] = None) -> List[NewsArticle]:
    sources: List[Callable[[], List[NewsArticle]]] = [
        fetch_empire_news,
        fetch_screenrant_news,
        lambda: fetch_newsdata_io(api_key)
    ]

    articles: List[NewsArticle] = []
    for fetch in sources:
        articles.extend(fetch())

    # Normalize all published timestamps to UTC
    for article in articles:
        if article.published and article.published.tzinfo is None:
            article.published = article.published.replace(tzinfo=timezone.utc)

    return sorted(articles, key=lambda x: x.published, reverse=True)
