# ─── Standard Library Imports ────────────────────────────────────────────────
import os
import sys
from datetime import datetime, timedelta

# ─── Third-Party Imports ─────────────────────────────────────────────────────
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Request, APIRouter
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# ─── Internal Project Imports ────────────────────────────────────────────────
from config.settings import TMDB_API_KEY
from db.connection import get_connection

# Uploader modules
from uploader.movie_uploader import insert_or_update_movie_data
from uploader.tv_uploader import insert_or_update_series_data

# TMDB API modules
from tmdb.movie_api import get_movie_data
from tmdb.tv_api import fetch_series

# ─── Standard Library Imports ────────────────────────────────────────────────
import os
import sys
from datetime import datetime, timedelta

# ─── Third-Party Imports ─────────────────────────────────────────────────────
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Request, APIRouter
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# ─── Internal Project Imports ────────────────────────────────────────────────
from config.settings import TMDB_API_KEY
from db.connection import get_connection

# Uploader modules
from uploader.movie_uploader import insert_or_update_movie_data
from uploader.tv_uploader import insert_or_update_series_data

# TMDB API modules
from tmdb.movie_api import get_movie_data
from tmdb.tv_api import fetch_series

# Services
from services import stats

# ─── Environment Setup ───────────────────────────────────────────────────────
# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Load environment variables
load_dotenv()

# ─── FastAPI App Initialization ──────────────────────────────────────────────
app = FastAPI()

# Static files and templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="web_ui/templates")

# ─── Router Setup ────────────────────────────────────────────────────────────
router = APIRouter()

@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request,
        "active_release_years": stats.get_active_release_years(),
        "hidden_gems": stats.get_hidden_gems(),
        "most_reviewed_titles": stats.get_most_reviewed_titles(),
        "popular_genres": stats.get_popular_genres(),
        "prolific_actors": stats.get_prolific_actors(),
        "top_rated_actors": stats.get_top_rated_actors(),
        "top_rated_movies": stats.get_top_rated_movies(),
        "trending_titles": stats.get_trending_titles(),
    })

# ─── Register Router ─────────────────────────────────────────────────────────
app.include_router(router)

def classify_freshness(lastupdated):
    if not lastupdated:
        return "stale"  # treat missing as stale

    now = datetime.utcnow()
    delta = now - lastupdated

    if delta <= timedelta(days=7):
        return "fresh"
    elif delta <= timedelta(days=30):
        return "moderate"
    else:
        return "stale"

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Load environment variables
load_dotenv()

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="web_ui/templates")

@app.post("/search_person", response_class=HTMLResponse)
async def search_person(request: Request):
    form = await request.form()
    name = form.get("person_name")
    people = search_person_tmdb(name)
    return templates.TemplateResponse("person_results.html", {"request": request, "people": people})

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    conn = get_connection()
    stats = {}
    try:
        with conn.cursor() as cur:
            # Basic counts
            cur.execute("SELECT COUNT(*) FROM movies;")
            stats["movie_count"] = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM series;")
            stats["series_count"] = cur.fetchone()[0]

            # Last update timestamp
            cur.execute("SELECT MAX(timestamp) FROM update_logs;")
            stats["last_update"] = cur.fetchone()[0]

            # Recent updates
            cur.execute("SELECT COUNT(*) FROM update_logs WHERE timestamp >= %s;", 
                        (datetime.utcnow() - timedelta(days=7),))
            stats["recent_updates"] = cur.fetchone()[0]

            # Freshness breakdown
            cur.execute("SELECT lastupdated FROM movies UNION ALL SELECT lastupdated FROM series;")
            freshness_counts = {"fresh": 0, "moderate": 0, "stale": 0}
            for row in cur.fetchall():
                freshness = classify_freshness(row[0])
                freshness_counts[freshness] += 1
            stats["freshness"] = freshness_counts

            # Most updated title
            cur.execute("""
                SELECT movie_id, COUNT(*) AS changes
                FROM update_logs
                GROUP BY movie_id
                ORDER BY changes DESC
                LIMIT 1;
            """)
            stats["most_updated_id"] = cur.fetchone()[0]

            # Most changed fields
            cur.execute("""
                SELECT field_name, COUNT(*) AS freq
                FROM update_logs
                GROUP BY field_name
                ORDER BY freq DESC
                LIMIT 5;
            """)
            stats["top_fields"] = cur.fetchall()

            # Missing key fields
            cur.execute("SELECT COUNT(*) FROM movies WHERE overview IS NULL OR release_date IS NULL;")
            stats["movies_missing_fields"] = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM series WHERE overview IS NULL OR first_air_date IS NULL;")
            stats["series_missing_fields"] = cur.fetchone()[0]

            # Orphaned logs
            cur.execute("""
                SELECT COUNT(*) FROM update_logs
                WHERE movie_id NOT IN (
                    SELECT id FROM movies
                    UNION
                    SELECT series_id FROM series
                );
            """)
            stats["orphaned_logs"] = cur.fetchone()[0]

            # Active release years
            cur.execute("""
                SELECT mad.release_year, COUNT(*) AS title_count
                FROM movies m
                JOIN movie_additional_details mad ON mad.movie_id = m.id
                GROUP BY mad.release_year
                ORDER BY title_count DESC
                LIMIT 10;
            """)
            stats["active_release_years"] = cur.fetchall()

            # Hidden gems
            cur.execute("""
                SELECT id, title, vote_average, vote_count
                FROM movies
                WHERE vote_average >= 7.0 AND vote_count > 100 AND vote_count < 1000
                ORDER BY vote_average DESC
                LIMIT 10;
            """)
            hidden_gems = [
                {
                    "id": row[0],
                    "title": row[1],
                    "vote_average": row[2],
                    "vote_count": row[3]
                }
                for row in cur.fetchall()
            ]

            # Most reviewed titles
            cur.execute("""
                SELECT id, title, vote_count
                FROM movies
                WHERE vote_count IS NOT NULL
                ORDER BY vote_count DESC
                LIMIT 10;
            """)
            most_reviewed_titles = [
                {
                    "id": row[0],
                    "title": row[1],
                    "vote_count": row[2]
                }
                for row in cur.fetchall()
            ]

            # Popular genres
            cur.execute("""
                SELECT g.name AS genre, COUNT(*) AS genre_count
                FROM genres g
                JOIN movie_genres mg ON mg.genre_id = g.id
                GROUP BY g.name
                ORDER BY genre_count DESC
                LIMIT 10;
            """)
            popular_genres = [
                {
                    "genre": row[0],
                    "genre_count": row[1]
                }
                for row in cur.fetchall()
            ]

            # Prolific actors
            cur.execute("""
                SELECT p.name AS actor_name, COUNT(*) AS appearances
                FROM moviecast mc
                JOIN people p ON mc.person_id = p.person_id
                GROUP BY p.name
                ORDER BY appearances DESC
                LIMIT 10;
            """)
            prolific_actors = [
                {
                    "actor_name": row[0],
                    "appearances": row[1]
                }
                for row in cur.fetchall()
            ]

            # top rated actors
            cur.execute("""
                SELECT p.name AS actor_name,
                    ROUND(AVG(m.vote_average)::numeric, 2) AS avg_rating,
                    COUNT(*) AS title_count
                FROM moviecast mc
                JOIN people p ON mc.person_id = p.person_id
                JOIN movies m ON mc.movie_id = m.id
                WHERE m.vote_average IS NOT NULL
                GROUP BY p.name
                HAVING COUNT(*) > 5
                ORDER BY avg_rating DESC
                LIMIT 10;
            """)
            top_rated_actors = [
                {
                    "actor_name": row[0],
                    "avg_rating": row[1],
                    "title_count": row[2]
                }
                for row in cur.fetchall()
            ]

            # top rated movies
            cur.execute("""
                SELECT id, title, vote_average
                FROM movies
                WHERE vote_average IS NOT NULL
                ORDER BY vote_average DESC
                LIMIT 10;
            """)
            top_rated_movies = [
                {
                    "id": row[0],
                    "title": row[1],
                    "vote_average": row[2]
                }
                for row in cur.fetchall()
            ]


    finally:
        conn.close()

    return templates.TemplateResponse("index.html", {
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
        "top_rated_movies": top_rated_movies

    })


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

    conn = get_connection()
    annotated_results = []
    try:
        with conn.cursor() as cur:
            for result in results:
                tmdb_id = result.get("id")
                media_type = result.get("media_type")

                try:
                    if media_type == "movie":
                        cur.execute("SELECT lastupdated FROM movies WHERE id = %s;", (tmdb_id,))
                    elif media_type == "tv":
                        cur.execute("SELECT lastupdated FROM series WHERE series_id = %s;", (tmdb_id,))
                    else:
                        result["exists"] = False
                        result["lastupdated"] = None
                        annotated_results.append(result)
                        continue

                    row = cur.fetchone()
                    result["exists"] = bool(row)
                    result["lastupdated"] = row[0] if row else None
                    result["freshness"] = classify_freshness(result["lastupdated"])

                except Exception as e:
                    print(f"❌ DB check failed for {media_type} ID {tmdb_id}: {e}")
                    result["exists"] = False
                    result["lastupdated"] = None

                annotated_results.append(result)
    finally:
        conn.close()

    return templates.TemplateResponse(
        "search_results.html", {"request": request, "results": annotated_results}
    )

@app.get("/upload", response_class=HTMLResponse)
async def upload(request: Request, tmdb_id: int, media_type: str):
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            # Step 1: Get latest timestamp before upload
            cur.execute(
                "SELECT MAX(timestamp) FROM update_logs WHERE movie_id = %s;",
                (tmdb_id,)
            )
            previous_max = cur.fetchone()[0] or datetime.min

        # Step 2: Perform upload
        if media_type == "movie":
            movie_data = get_movie_data(tmdb_id)
            insert_or_update_movie_data(conn, movie_data)
            base_message = f"✅ Movie '{movie_data.get('title')}' processed successfully."
            movie_id = movie_data["id"]
        elif media_type == "tv":
            series_data = fetch_series(tmdb_id)
            insert_or_update_series_data(conn, series_data, TMDB_API_KEY)
            base_message = f"✅ TV Series '{series_data.get('name')}' processed successfully."
            movie_id = series_data["id"]
        else:
            base_message = "❌ Invalid media type selected."
            movie_id = None

        # Step 3: Fetch only new logs
        changes = []
        if movie_id:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT timestamp, change_type, field_name, old_value, new_value
                    FROM update_logs
                    WHERE movie_id = %s AND timestamp > %s
                    ORDER BY timestamp DESC;
                    """,
                    (movie_id, previous_max),
                )
                logs = cur.fetchall()

            for log in logs:
                timestamp, change_type, field, old, new = log
                changes.append({
                    "timestamp": timestamp,
                    "change_type": change_type,
                    "field": field,
                    "old_value": old,
                    "new_value": new,
                })

        filtered_changes = filter_changes(changes)
        conn.close()

        message = base_message if filtered_changes else f"{base_message} No changes needed — already up-to-date."

    except Exception as e:
        message = f"❌ Error occurred: {str(e)}"
        filtered_changes = []

    return templates.TemplateResponse(
        "result.html", {"request": request, "message": message, "changes": filtered_changes}
    )

def classify_freshness(lastupdated):
    if not lastupdated:
        return "stale"  # treat missing as stale

    now = datetime.utcnow()
    delta = now - lastupdated

    if delta <= timedelta(days=7):
        return "fresh"
    elif delta <= timedelta(days=30):
        return "moderate"
    else:
        return "stale"

def filter_changes(raw_changes):
    """
    Filters out changes where old_value == new_value or both are empty.
    Formats timestamps for display.
    """
    filtered = []
    for change in raw_changes:
        old = change.get("old_value")
        new = change.get("new_value")

        if (old is None and new is None) or (str(old).strip() == "" and str(new).strip() == ""):
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
                detail_response = requests.get(detail_url, params={"api_key": TMDB_API_KEY})
                if detail_response.status_code == 200:
                    details = detail_response.json()
                    person["biography"] = details.get("biography")
                    person["birthday"] = details.get("birthday")
                    person["place_of_birth"] = details.get("place_of_birth")
                    person["also_known_as"] = details.get("also_known_as")

                # Fetch credits
                credits_url = f"https://api.themoviedb.org/3/person/{person_id}/combined_credits"
                credits_response = requests.get(credits_url, params={"api_key": TMDB_API_KEY})
                if credits_response.status_code == 200:
                    credits = credits_response.json().get("cast", [])
                    for credit in credits:
                        tmdb_id = credit.get("id")
                        media_type = credit.get("media_type")
                        date_str = credit.get("release_date") if media_type == "movie" else credit.get("first_air_date")

                        # Check existence in DB
                        if media_type == "movie":
                            cur.execute("SELECT 1 FROM movies WHERE id = %s;", (tmdb_id,))
                        elif media_type == "tv":
                            cur.execute("SELECT 1 FROM series WHERE series_id = %s;", (tmdb_id,))
                        else:
                            credit["exists"] = False
                            credit["sort_date"] = None
                            continue

                        credit["exists"] = cur.fetchone() is not None

                        # Parse date for sorting
                        try:
                            credit["sort_date"] = datetime.strptime(date_str, "%Y-%m-%d") if date_str else None
                        except Exception:
                            credit["sort_date"] = None

                    # Sort credits in descending order
                    credits.sort(key=lambda x: x.get("sort_date") or datetime.min, reverse=True)
                    person["credits"] = credits
    finally:
        conn.close()

    return people