# ─── Standard Library Imports ────────────────────────────────────────────────
import os
import sys
from datetime import datetime, timedelta

# ─── Third-Party Imports ─────────────────────────────────────────────────────
import requests
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
from uploader.tv_uploader import insert_or_update_series_data
from tmdb.movie_api import get_movie_data
from tmdb.tv_api import fetch_series
from services import stats
from services.freshness import get_freshness_summary  # or wherever you define it
from services.titles import get_title_by_id
from services.diagnostics import wrap_query

# from services.reviews import get_reviews_for_title
from services.actors import get_cast_for_title

# ─── Environment Setup ───────────────────────────────────────────────────────
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv()

# ─── FastAPI App Initialization ──────────────────────────────────────────────
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="web_ui/templates")

# ─── Router Setup ────────────────────────────────────────────────────────────
router = APIRouter()


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
            "movie_title": title,
            "cast": cast,
            "diagnostics": diagnostics,
            # "reviews": reviews
        },
    )


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", get_stats_context(request))


@router.get("/uploader", response_class=HTMLResponse, name="uploader")
async def uploader(request: Request):
    return templates.TemplateResponse("uploader.html", get_stats_context(request))


def get_stats_context(request: Request):
    return {
        "request": request,
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
        "person_results.html", {"request": request, "people": people}
    )


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

            # Last update logged_at
            cur.execute("SELECT MAX(logged_at) FROM update_logs;")
            stats["last_update"] = cur.fetchone()[0]

            # Recent updates
            cur.execute(
                "SELECT COUNT(*) FROM update_logs WHERE logged_at >= %s;",
                (datetime.utcnow() - timedelta(days=7),),
            )
            stats["recent_updates"] = cur.fetchone()[0]

            # Freshness breakdown
            cur.execute(
                "SELECT last_updated FROM movies UNION ALL SELECT last_updated FROM series;"
            )
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
                SELECT updated_field, COUNT(*) AS freq
                FROM update_logs
                GROUP BY updated_field 
                ORDER BY freq DESC
                LIMIT 5;
            """)
            stats["top_fields"] = cur.fetchall()

            # Missing key fields
            cur.execute(
                "SELECT COUNT(*) FROM movies WHERE overview IS NULL OR release_date IS NULL;"
            )
            stats["movies_missing_fields"] = cur.fetchone()[0]

            cur.execute(
                "SELECT COUNT(*) FROM series WHERE overview IS NULL OR first_air_date IS NULL;"
            )
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
        "search_results.html", {"request": request, "results": annotated_results}
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

    try:
        conn = get_connection()
        previous_max = get_previous_log_timestamp(conn, tmdb_id)

        movie_id, base_message = process_media_upload(conn, tmdb_id, media_type)

        if movie_id:
            changes = fetch_new_update_logs(conn, movie_id, previous_max)
            filtered_changes = filter_changes(changes)
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

    print("🧪 Filtered changes:", filtered_changes)

    return templates.TemplateResponse(
        "result.html",
        {
            "request": request,
            "message": message,
            "changes": filtered_changes,
        },
    )


def get_previous_log_timestamp(conn, movie_id):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT MAX(logged_at) FROM update_logs WHERE movie_id = %s;",
            (movie_id,),
        )
        return cur.fetchone()[0] or datetime.min


def process_media_upload(conn, tmdb_id, media_type):
    if media_type == "movie":
        movie_data = get_movie_data(tmdb_id)
        insert_or_update_movie_data(conn, movie_data)
        return movie_data[
            "id"
        ], f"✅ Movie '{movie_data.get('title')}' processed successfully."

    elif media_type == "tv":
        series_data = fetch_series(tmdb_id)
        insert_or_update_series_data(conn, series_data, TMDB_API_KEY)
        return series_data[
            "id"
        ], f"✅ TV Series '{series_data.get('name')}' processed successfully."

    return None, "❌ Invalid media type selected."


def fetch_new_update_logs(conn, movie_id, since):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT logged_at, update_type, updated_field, previous_value, current_value
            FROM update_logs
            WHERE movie_id = %s AND logged_at > %s
            ORDER BY logged_at DESC;
        """,
            (movie_id, since),
        )
        logs = cur.fetchall()

    return [
        {
            "logged_at": log[0],
            "update_type": log[1],
            "updated_field": log[2],
            "previous_value": log[3],
            "current_value": log[4],
        }
        for log in logs
    ]


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
    Filters out changes where previous_value == current_value or both are empty.
    Formats logged_at for display.
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

        ts = change.get("logged_at")
        if isinstance(ts, datetime):
            change["logged_at"] = ts.strftime("%Y-%m-%d %H:%M:%S")

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
