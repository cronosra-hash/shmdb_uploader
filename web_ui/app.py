# ─── Standard Library Imports ────────────────────────────────────────────────
import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


# ─── Third-Party Imports ─────────────────────────────────────────────────────
import requests
from typing import List, Dict
from dotenv import load_dotenv
from fastapi import FastAPI, Request, APIRouter
from fastapi.responses import HTMLResponse
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# ─── Internal Project Imports ────────────────────────────────────────────────
from config.settings import TMDB_API_KEY
from db.connection import get_connection
from uploader.movie_uploader import insert_or_update_movie_data
from uploader.tv_uploader import insert_or_update_series_data, sync_series_episodes, sync_series_seasons
from tmdb.movie_api import get_movie_data
from tmdb.tv_api import fetch_series, fetch_all_episodes
from services import stats
from services.freshness import get_freshness_summary  # or wherever you define it
from services.titles import get_title_by_id, get_movie_titles_missing, get_tv_titles_missing
from services.diagnostics import wrap_query
from web_ui.filters import datetimeformat, ago, to_timezone, timestamp_color
from routes import news
from services.news_fetcher import get_all_news
from services.stats import get_new_releases

# from services.reviews import get_reviews_for_title
from services.actors import get_cast_for_title

# ─── Environment Setup ───────────────────────────────────────────────────────
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv()
APP_ENV = os.getenv("APP_ENV", "unknown")

# ─── FastAPI App Initialization ──────────────────────────────────────────────
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="web_ui/templates")
templates.env.filters["datetimeformat"] = datetimeformat
templates.env.filters["ago"] = ago
templates.env.filters["to_timezone"] = to_timezone
templates.env.filters["timestamp_color"] = timestamp_color

# ─── Router Setup ────────────────────────────────────────────────────────────
router = APIRouter()

app.include_router(news.router)

@app.get("/missing/{field}", name="missing_movies")
async def missing_movies(request: Request, field: str):
    movies = get_movie_titles_missing(field)
    return templates.TemplateResponse("partials/missing_movies.html", {
        "request": request,
        "field": field,
        "movies": movies
    })

@app.get("/missing_tv/{field}", name="missing_tv")
async def missing_tv(request: Request, field: str):
    titles = get_tv_titles_missing(field)
    return templates.TemplateResponse("partials/missing_tv.html", {
        "request": request,
        "field": field,
        "titles": titles
    })

@router.get("/title/{title_id}", response_class=HTMLResponse)
async def title_detail(request: Request, title_id: int):
    diagnostics = wrap_query("get_title_by_id", lambda: [get_title_by_id(title_id)])

    title = diagnostics["data"][0] if diagnostics["record_count"] else None
    cast = get_cast_for_title(title_id)
    # reviews = get_reviews_for_title(title_id)

    return templates.TemplateResponse(
        "title_detail.html",
        {
            "request": request,
            "title": title,
            "cast": cast,
            "diagnostics": diagnostics,
            "now": datetime.now()
            # "reviews": reviews
        },
    )

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    now = datetime.now()
    next_month = (now.replace(day=1) + timedelta(days=32)).replace(day=1)

    current_releases = get_cinema_releases(month=now.month, year=now.year)
    next_month_releases = get_cinema_releases(month=next_month.month, year=next_month.year)

    return templates.TemplateResponse("index.html", {
        **get_stats_context(request),
        "app_env": APP_ENV,
        "app_version": "0.1.0",
        "now": now,
        "next_month": next_month,
        "cinema_releases": current_releases,
        "next_month_releases": next_month_releases,
        "tv_releases": get_tv_releases(month=now.month, year=now.year),
    })

@router.get("/statistics", response_class=HTMLResponse, name="statistics")
async def statistics(request: Request):
    return templates.TemplateResponse("statistics.html", get_stats_context(request))

@router.get("/uploader", response_class=HTMLResponse, name="uploader")
async def uploader(request: Request):
    return templates.TemplateResponse("uploader.html", get_stats_context(request))

@app.get("/news")
def news_page(request: Request):
    articles = get_all_news(api_key="pub_000738d4a1274d798638038b9633580c")
    return templates.TemplateResponse("news.html", {
        "request": request,
        "articles": articles
    })


def get_stats_context(request: Request):
    articles = get_all_news(api_key="pub_000738d4a1274d798638038b9633580c")
    new_releases = get_new_releases()

    return {
        "request": request,
        "now": datetime.now(),
        "app_env": APP_ENV,
        "app_version": "0.1.0",
        "articles": articles,
        "new_releases": new_releases,
        "top_fields": stats.get_top_fields(),
        "movie_count": stats.get_movie_count(),
        "series_count": stats.get_series_count(),
        "last_update": stats.get_last_update(),
        "recent_updates": stats.get_recent_updates(),
        "most_updated_title": stats.get_most_updated_title(),
        "movies_missing_fields": stats.get_movies_missing_fields(),
        "series_missing_fields": stats.get_series_missing_fields(),
        "orphaned_logs": stats.get_orphaned_logs(),
        "stats": {
            "active_release_years": stats.get_active_release_years(),
            "hidden_gems": stats.get_hidden_gems(),
            "most_reviewed_titles": stats.get_most_reviewed_titles(),
            "popular_genres": stats.get_popular_genres(),
            "prolific_actors": stats.get_prolific_actors(),
            "top_rated_actors": stats.get_top_rated_actors(),
            "top_rated_movies": stats.get_top_rated_movies(),
            "trending_titles": stats.get_trending_titles(),
            "freshness": get_freshness_summary(),
        },
    }


# ─── Register Router ─────────────────────────────────────────────────────────
app.include_router(router)


# ─── Utility Functions ───────────────────────────────────────────────────────
def classify_freshness(lastupdated):
    if not lastupdated:
        return "stale"
    now = datetime.utcnow()
    delta = now - lastupdated
    if delta <= timedelta(days=7):
        return "fresh"
    elif delta <= timedelta(days=30):
        return "moderate"
    else:
        return "stale"


@app.post("/search_person", response_class=HTMLResponse)
async def search_person(request: Request):
    form = await request.form()
    name = form.get("person_name")
    people = search_person_tmdb(name)
    return templates.TemplateResponse(
        "person_results.html", {"request": request, "people": people, "now": datetime.now(),}
    )


@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    conn = get_connection()
    stats = {}
    try:
        with conn.cursor() as cur:
            # Orphaned logs
            cur.execute("""
                SELECT COUNT(*) FROM update_logs
                WHERE content_id NOT IN (
                SELECT movie_id FROM movies
                UNION
                SELECT series_id FROM series);
            """)
            stats["orphaned_logs"] = cur.fetchone()[0]

            # Active release years
            cur.execute("""
                SELECT mmd.release_year, COUNT(*) AS title_count
                FROM movies m
                JOIN movie_metadata mmd ON mmd.movie_id = m.movie_id
                GROUP BY mmd.release_year
                ORDER BY title_count DESC
                LIMIT 10;
            """)
            stats["active_release_years"] = cur.fetchall()

            # Hidden gems
            cur.execute("""
                SELECT movie_id, movie_title, vote_average, vote_count
                FROM movies
                WHERE vote_average >= 7.0 AND vote_count > 100 AND vote_count < 1000
                ORDER BY vote_average DESC
                LIMIT 10;
            """)
            hidden_gems = [
                {
                    "movie_id": row[0],
                    "movie_title": row[1],
                    "vote_average": row[2],
                    "vote_count": row[3],
                }
                for row in cur.fetchall()
            ]

            # Most reviewed titles
            cur.execute("""
                SELECT movie_id, movie_title, vote_count
                FROM movies
                WHERE vote_count IS NOT NULL
                ORDER BY vote_count DESC
                LIMIT 10;
            """)
            most_reviewed_titles = [
                {"movie_id": row[0], "movie_title": row[1], "vote_count": row[2]}
                for row in cur.fetchall()
            ]

            # Popular genres
            cur.execute("""
                SELECT g.genre_name AS genre, COUNT(*) AS genre_count
                FROM genres g
                JOIN movie_genres mg ON mg.genre_id = g.genre_id
                GROUP BY g.genre_name
                ORDER BY genre_count DESC
                LIMIT 10;
            """)
            popular_genres = [
                {"genre_name": row[0], "genre_count": row[1]} for row in cur.fetchall()
            ]

            # Prolific actors
            cur.execute("""
                SELECT p.name AS actor_name, COUNT(*) AS appearances
                FROM movie_cast mc
                JOIN people p ON mc.actor_id = p.person_id
                GROUP BY p.name
                ORDER BY appearances DESC
                LIMIT 10;
            """)
            prolific_actors = [
                {"actor_name": row[0], "appearances": row[1]} for row in cur.fetchall()
            ]

            # top rated actors
            cur.execute("""
                SELECT p.name AS actor_name,
                    ROUND(AVG(m.vote_average)::numeric, 2) AS avg_rating,
                    COUNT(*) AS title_count
                FROM movie_cast mc
                JOIN people p ON mc.actor_id = p.person_id
                JOIN movies m ON mc.movie_id = m.movie_id
                WHERE m.vote_average IS NOT NULL
                GROUP BY p.name
                HAVING COUNT(*) > 5
                ORDER BY avg_rating DESC
                LIMIT 10;
            """)
            top_rated_actors = [
                {"actor_name": row[0], "avg_rating": row[1], "title_count": row[2]}
                for row in cur.fetchall()
            ]

            # top rated movies
            cur.execute("""
                SELECT movie_id, movie_title, vote_average
                FROM movies
                WHERE vote_average IS NOT NULL
                ORDER BY vote_average DESC
                LIMIT 10;
            """)
            top_rated_movies = [
                {"id": row[0], "movie_title": row[1], "vote_average": row[2]}
                for row in cur.fetchall()
            ]

            # trending titles
            cur.execute("""
                SELECT movie_id, movie_title, last_updated
                FROM movies
                WHERE last_updated > NOW() - INTERVAL '7 days'
                ORDER BY last_updated DESC
                LIMIT 10;
            """)
            trending_titles = [
                {"movie_id": row[0], "movie_title": row[1], "last_updated": row[2]}
                for row in cur.fetchall()
            ]

    finally:
        conn.close()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "stats": stats,
            "active_release_years": [
                {"release_year": row[0], "title_count": row[1]}
                for row in stats["active_release_years"]
            ],
            "hidden_gems": hidden_gems,
            "most_reviewed_titles": most_reviewed_titles,
            "popular_genres": popular_genres,
            "prolific_actors": prolific_actors,
            "top_rated_actors": top_rated_actors,
            "top_rated_movies": top_rated_movies,
            "trending_titles": trending_titles,
        },
    )



@app.post("/search", response_class=HTMLResponse)
async def search(request: Request):
    form = await request.form()
    tmdb_id = form.get("tmdb_id")
    name = form.get("name")

    if tmdb_id:
        media_type = form.get("media_type")
        return RedirectResponse(
            url=f"/upload?tmdb_id={tmdb_id}&media_type={media_type}", status_code=303
        )

    results = search_tmdb_combined(name)
    annotated_results = annotate_results_with_db_status(results)

    return templates.TemplateResponse(
        "search_results.html", {"request": request, "results": annotated_results, "now": datetime.now()}
    )


def annotate_results_with_db_status(results):
    conn = get_connection()
    annotated = []

    try:
        with conn.cursor() as cur:
            for result in results:
                annotated.append(annotate_result(cur, result))
    finally:
        conn.close()

    return annotated


def annotate_result(cur, result):
    tmdb_id = result.get("id")
    media_type = result.get("media_type")

    try:
        if media_type == "movie":
            cur.execute(
                "SELECT last_updated FROM movies WHERE movie_id = %s;", (tmdb_id,)
            )
        elif media_type == "tv":
            cur.execute(
                "SELECT last_updated FROM series WHERE series_id = %s;", (tmdb_id,)
            )
        else:
            return {**result, "exists": False, "last_updated": None}

        row = cur.fetchone()
        last_updated = row[0] if row else None

        return {
            **result,
            "exists": bool(row),
            "last_updated": last_updated,
            "last_updated_local": format_local(last_updated),
            "freshness": classify_freshness(last_updated),
        }

    except Exception as e:
        print(f"❌ DB check failed for {media_type} ID {tmdb_id}: {e}")
        return {**result, "exists": False, "last_updated": None}


@app.get("/upload", response_class=HTMLResponse)
async def upload(request: Request, tmdb_id: int, media_type: str):
    conn = None
    filtered_changes = []
    message = ""
    title = ""
    content_id = None
    changes = []

    try:
        conn = get_connection()
        previous_max = get_previous_log_timestamp(conn, tmdb_id, media_type)
        if previous_max is None:
            print("🧪 No previous log found — defaulting to datetime.min")
            previous_max = datetime.min

        content_id, base_message = process_media_upload(conn, tmdb_id, media_type)

        if content_id:
            print("🧪 Forcing log fetch...")
            changes = fetch_new_update_logs(conn, content_id, media_type, previous_max)

            filtered_changes = filter_changes(changes)

            # Extract title from first change (if available)
            if filtered_changes:
                title = filtered_changes[0].get("title") or filtered_changes[0].get(
                    "movie_title", ""
                )

            message = (
                base_message
                if filtered_changes
                else f"{base_message} No changes needed — already up-to-date."
            )
        else:
            message = f"{base_message} No movie ID returned — upload may have failed."

    except Exception as e:
        message = f"❌ Error occurred: {str(e)}"

    finally:
        if conn:
            conn.close()

    return templates.TemplateResponse(
        "result.html",
        {
            "request": request,
            "message": message,
            "changes": filtered_changes,
            "title": title,
            "content_id": content_id,
            "media_type": media_type,
            "upload_status": "Upload complete",  # optional
            "raw_changes": changes,
            "now": datetime.now(),
        }
    )


def get_previous_log_timestamp(conn, content_id, content_type):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(timestamp)
            FROM update_logs
            WHERE content_id = %s AND content_type = %s;
            """,
            (content_id, content_type),
        )
        result = cur.fetchone()
        return result[0] or datetime.min


def process_media_upload(conn, tmdb_id, media_type):
    if media_type == "movie":
        movie_data = get_movie_data(tmdb_id)
        insert_or_update_movie_data(conn, movie_data, media_type)
        print(f"🎬 Movie '{movie_data.get('title')}' synced (id={movie_data['id']})")
        return movie_data["id"], f"✅ Movie '{movie_data.get('title')}' processed successfully."

    elif media_type == "tv":
        series_data = fetch_series(tmdb_id)
        series_id = series_data.get("id")
        series_name = series_data.get("name")

        print(f"📺 Starting sync for TV Series '{series_name}' (id={series_id})")

        # ✅ Insert/update series first to satisfy foreign key constraints
        insert_or_update_series_data(conn, series_data, TMDB_API_KEY)

        # 🔄 Now insert seasons
        with conn.cursor() as cur:
            sync_series_seasons(cur, series_data)

        # 🔄 Fetch and attach episodes
        episodes = fetch_all_episodes(series_id)
        if not episodes:
            print(f"⚠️ No episodes found for series_id={series_id}")
        else:
            print(f"📦 {len(episodes)} episodes fetched for series_id={series_id}")
        series_data["episodes"] = episodes

        # 🔄 Sync episodes
        with conn.cursor() as cur:
            sync_series_episodes(cur, series_id, series_data)

        conn.commit()  # ✅ Ensure inserts are persisted

        print(f"✅ TV Series '{series_name}' synced successfully")
        return series_id, f"✅ TV Series '{series_name}' processed successfully."

    print(f"❌ Unsupported media type: {media_type}")
    return None, "❌ Invalid media type selected."


def fetch_new_update_logs(conn, content_id, content_type, since):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT timestamp, update_type, field_name, previous_value, current_value
            FROM update_logs
            WHERE content_id = %s AND content_type = %s AND timestamp > %s
            ORDER BY timestamp DESC;
            """,
            (content_id, content_type, since),
        )
        logs = cur.fetchall()
    return [
        {
            "timestamp": log[0],
            "update_type": log[1],
            "field_name": log[2],
            "previous_value": log[3],
            "current_value": log[4],
        }
        for log in logs
    ]


def filter_changes(raw_changes):
    """
    Filters out changes where previous_value == current_value or both are empty.
    Formats timestamp for display.
    """
    filtered = []
    for change in raw_changes:
        old = change.get("previous_value")
        new = change.get("current_value")

        if (old is None and new is None) or (
            str(old).strip() == "" and str(new).strip() == ""
        ):
            continue
        if str(old) == str(new):
            continue

        ts = change.get("timestamp")
        if isinstance(ts, datetime):
            change["timestamp"] = ts.strftime("%Y-%m-%d %H:%M:%S")

        filtered.append(change)

    return filtered


def search_tmdb_combined(name):
    endpoints = ["movie", "tv"]
    combined_results = []

    for media_type in endpoints:
        url = f"https://api.themoviedb.org/3/search/{media_type}"
        params = {"api_key": TMDB_API_KEY, "query": name, "include_adult": False}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            results = response.json().get("results", [])
            for result in results:
                result["media_type"] = media_type

                # Enrich with imdb_id
                detail_url = (
                    f"https://api.themoviedb.org/3/movie/{result['id']}"
                    if media_type == "movie"
                    else f"https://api.themoviedb.org/3/tv/{result['id']}/external_ids"
                )
                detail_response = requests.get(detail_url, params={"api_key": TMDB_API_KEY})
                if detail_response.status_code == 200:
                    detail_data = detail_response.json()
                    result["imdb_id"] = detail_data.get("imdb_id")

                combined_results.append(result)

    combined_results.sort(key=lambda x: x.get("popularity", 0), reverse=True)
    return combined_results

def search_person_tmdb(name):
    url = "https://api.themoviedb.org/3/search/person"
    params = {"api_key": TMDB_API_KEY, "query": name}
    response = requests.get(url, params=params)
    if response.status_code != 200:
        return []

    people = response.json().get("results", [])
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for person in people:
                person_id = person.get("id")

                # Fetch detailed info
                detail_url = f"https://api.themoviedb.org/3/person/{person_id}"
                detail_response = requests.get(
                    detail_url, params={"api_key": TMDB_API_KEY}
                )
                if detail_response.status_code == 200:
                    details = detail_response.json()
                    person["biography"] = details.get("biography")
                    person["birthday"] = details.get("birthday")
                    person["place_of_birth"] = details.get("place_of_birth")
                    person["also_known_as"] = details.get("also_known_as")

                # Fetch credits
                credits_url = (
                    f"https://api.themoviedb.org/3/person/{person_id}/combined_credits"
                )
                credits_response = requests.get(
                    credits_url, params={"api_key": TMDB_API_KEY}
                )
                if credits_response.status_code == 200:
                    credits = credits_response.json().get("cast", [])
                    for credit in credits:
                        tmdb_id = credit.get("id")
                        media_type = credit.get("media_type")
                        date_str = (
                            credit.get("release_date")
                            if media_type == "movie"
                            else credit.get("first_air_date")
                        )

                        # Check existence in DB
                        if media_type == "movie":
                            cur.execute(
                                "SELECT 1 FROM movies WHERE movie_id = %s;", (tmdb_id,)
                            )
                        elif media_type == "tv":
                            cur.execute(
                                "SELECT 1 FROM series WHERE series_id = %s;", (tmdb_id,)
                            )
                        else:
                            credit["exists"] = False
                            credit["sort_date"] = None
                            continue

                        credit["exists"] = cur.fetchone() is not None

                        # Parse date for sorting
                        try:
                            credit["sort_date"] = (
                                datetime.strptime(date_str, "%Y-%m-%d")
                                if date_str
                                else None
                            )
                        except Exception:
                            credit["sort_date"] = None

                    # Sort credits in descending order
                    credits.sort(
                        key=lambda x: x.get("sort_date") or datetime.min, reverse=True
                    )
                    person["credits"] = credits
    finally:
        conn.close()

    return people

TMDB_BASE = "https://api.themoviedb.org/3"

# Cache genre maps
_movie_genres = None
_tv_genres = None

def get_movie_details(movie_id: int) -> Dict:
    url = f"{TMDB_BASE}/movie/{movie_id}"
    params = {"api_key": TMDB_API_KEY, "language": "en-GB"}
    try:
        response = requests.get(url, params=params, timeout=5)
        return response.json()
    except Exception as e:
        print(f"Error fetching details for movie {movie_id}: {e}")
        return {}

def get_genre_map(content_type: str) -> Dict[int, str]:
    global _movie_genres, _tv_genres
    if content_type == "movie" and _movie_genres:
        return _movie_genres
    if content_type == "tv" and _tv_genres:
        return _tv_genres

    url = f"{TMDB_BASE}/genre/{content_type}/list"
    params = {"api_key": TMDB_API_KEY, "language": "en-GB"}
    response = requests.get(url, params=params)
    genres = response.json().get("genres", [])
    genre_map = {g["id"]: g["name"] for g in genres}

    if content_type == "movie":
        _movie_genres = genre_map
    else:
        _tv_genres = genre_map

    return genre_map

def get_uk_release_info(movie_id: int) -> Dict:
    url = f"{TMDB_BASE}/movie/{movie_id}/release_dates"
    params = {"api_key": TMDB_API_KEY}
    try:
        response = requests.get(url, params=params, timeout=5)
        data = response.json()
        for entry in data.get("results", []):
            if entry.get("iso_3166_1") == "GB":
                for release in entry.get("release_dates", []):
                    if release.get("type") in [2, 3]:  # Theatrical
                        return {
                            "date": release.get("release_date", "")[:10],
                            "certification": release.get("certification", "")
                        }
    except Exception as e:
        print(f"Error fetching UK release info for movie {movie_id}: {e}")
    return {"date": "", "certification": ""}

def get_month_range(month: int = None, year: int = None) -> List[str]:
    if month and year:
        return [f"{year}-{month:02d}"]
    now = datetime.now()
    current_releases = get_cinema_releases(month=now.month, year=now.year)
    next_month = (now.replace(day=1) + timedelta(days=32))
    next_month_releases = get_cinema_releases(month=next_month.month, year=next_month.year)
    return [
        current_releases.strftime("%Y-%m"),
        next_month.strftime("%Y-%m")
    ]

def get_cinema_releases(month: int = None, year: int = None) -> List[Dict]:
    genre_map = get_genre_map("movie")
    releases = []

    for ym in get_month_range(month, year):
        y, m = map(int, ym.split("-"))
        url = f"{TMDB_BASE}/discover/movie"
        params = {
            "api_key": TMDB_API_KEY,
            "language": "en-GB",
            "sort_by": "popularity.desc",
            "release_date.gte": f"{y}-{m:02d}-01",
            "release_date.lte": f"{y}-{m:02d}-31"
        }

        response = requests.get(url, params=params)
        movies = response.json().get("results", [])

        for movie in movies:
            release_info = get_uk_release_info(movie["id"])
            uk_date = release_info["date"]
            if isinstance(uk_date, str) and uk_date.startswith(f"{y}-{m:02d}"):
                details = get_movie_details(movie["id"])
                distributor = ""
                if details.get("production_companies"):
                    distributor = details["production_companies"][0].get("name", "")
                releases.append({
                    "title": movie["title"],
                    "release_date": format_local(uk_date),
                    "runtime": details.get("runtime", "Unknown"),
                    "certification": release_info.get("certification", "Unrated"),
                    "distributor": distributor or "Unknown",
                    "genre": " / ".join([genre_map.get(gid, "Unknown") for gid in movie.get("genre_ids", [])]),
                    "poster_path": movie.get("poster_path"),
                    "source": "TMDb",
                    "source_url": f"https://www.themoviedb.org/movie/{movie['id']}"
                })

    return releases

def get_tv_platform(tv_id: int) -> str:
    url = f"{TMDB_BASE}/tv/{tv_id}"
    params = {"api_key": TMDB_API_KEY, "language": "en-GB"}
    try:
        response = requests.get(url, params=params, timeout=5)
        data = response.json()
        networks = data.get("networks", [])
        return networks[0]["name"] if networks else "Unknown"
    except Exception:
        return "Unknown"

def get_tv_releases(month: int = None, year: int = None) -> List[Dict]:
    genre_map = get_genre_map("tv")
    releases = []

    for ym in get_month_range(month, year):
        y, m = map(int, ym.split("-"))
        url = f"{TMDB_BASE}/discover/tv"
        params = {
            "api_key": TMDB_API_KEY,
            "language": "en-GB",
            "sort_by": "first_air_date.asc",
            "first_air_date.gte": f"{y}-{m:02d}-01",
            "first_air_date.lte": f"{y}-{m:02d}-31"
        }

        response = requests.get(url, params=params)
        shows = response.json().get("results", [])

        for s in shows:
            if "GB" not in s.get("origin_country", []):
                continue

            # Fetch broadcaster info from TV details
            detail_url = f"{TMDB_BASE}/tv/{s['id']}"
            detail_params = {
                "api_key": TMDB_API_KEY,
                "language": "en-GB"
            }
            detail_response = requests.get(detail_url, params=detail_params)
            detail_data = detail_response.json()

            broadcasters = detail_data.get("networks", [])
            broadcaster_names = [b.get("name") for b in broadcasters if b.get("name")]
            broadcaster = ", ".join(broadcaster_names) if broadcaster_names else "Unknown"

            releases.append({
                "title": s["name"],
                "release_date": format_local(s.get("first_air_date")),
                "platform": broadcaster,
                "genre": " / ".join([genre_map.get(gid, "Unknown") for gid in s.get("genre_ids", [])]),
                "poster_path": s.get("poster_path"),
                "source": "TMDb",
                "source_url": f"https://www.themoviedb.org/tv/{s['id']}"
            })

    return releases

def format_local(dt, fmt="%d %b %Y, %H:%M"):
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except ValueError:
            return "Unknown"
    if not isinstance(dt, datetime):
        return "Unknown"
    return dt.astimezone(ZoneInfo("Europe/London")).strftime(fmt)

