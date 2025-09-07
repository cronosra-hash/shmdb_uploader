# routes/news.py
from datetime import datetime
from fastapi import APIRouter
from services.news_fetcher import get_all_news

# Now this works:
datetime.utcnow()

router = APIRouter()

@router.get("/news")
def news_endpoint():
    articles = get_all_news()
    print(f"Fetched {len(articles)} articles")  # Debug line
    return {
        "fetched_at": datetime.utcnow(),
        "article_count": len(articles),
        "articles": [article.dict() for article in articles]
    }
