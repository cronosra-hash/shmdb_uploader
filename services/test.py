import feedparser

feed = feedparser.parse("https://www.empireonline.com/movies/news/rss/")
print(f"Entries found: {len(feed.entries)}")
for entry in feed.entries[:3]:
    print(entry.title)
