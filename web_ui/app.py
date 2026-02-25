# ─── Standard Library Imports ────────────────────────────────────────────────
import os
import sys
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from collections import defaultdict

# ─── Third-Party Imports ─────────────────────────────────────────────────────
from psycopg2.extras import RealDictCursor
import requests
from typing import List, Dict
from dotenv import load_dotenv
from fastapi import FastAPI, Request, APIRouter, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import traceback

# ─── Internal Project Imports ────────────────────────────────────────────────
from config.settings import TMDB_API_KEY
from db.connection import get_connection
from db.helpers import dict_cursor
from uploader.media_processor import process_media_upload
from uploader.movie_uploader import insert_or_update_movie_data
from uploader.tv_uploader import (
    insert_or_update_series_data,
    sync_series_episodes,
    sync_series_seasons,
)
from tmdb.movie_api import get_movie_data
from tmdb.person_api import search_person_tmdb
from tmdb.search_api import search_tmdb_combined, get_tmdb_data
from tmdb.tv_api import fetch_series, fetch_all_episodes
from services.logs import (
    get_previous_log_timestamp,
    fetch_new_update_logs,
    filter_changes,
)
from services import stats
from services.freshness import get_freshness_summary
from services.releases import get_cinema_releases, get_tv_releases
from services.titles import (
    get_title_by_id,
    get_series_by_id,
    get_movie_titles_missing,
    get_tv_titles_missing,
)
from services.diagnostics import wrap_query
from web_ui.filters import datetimeformat, ago, to_timezone, timestamp_color
from routes import news
from services.news_fetcher import get_all_news
from services.stats import get_new_releases
from services.title_utils import get_related_titles
from services.actors import get_cast_for_title, get_crew_for_title

# ─── Environment Setup ───────────────────────────────────────────────────────
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
load_dotenv()
APP_ENV = os.getenv("APP_ENV", "unknown")

# ─── FastAPI App Initialization ──────────────────────────────────────────────
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")


def currency(value, symbol="$"):
    try:
        value = float(value)
        return f"{symbol}{value:,.0f}"
    except (ValueError, TypeError):
        return value


templates = Jinja2Templates(directory="web_ui/templates")
templates.env.filters["datetimeformat"] = datetimeformat
templates.env.filters["ago"] = ago
templates.env.filters["to_timezone"] = to_timezone
templates.env.filters["timestamp_color"] = timestamp_color
templates.env.filters["currency"] = currency

# ─── Router Setup ────────────────────────────────────────────────────────────
router = APIRouter()
app.include_router(news.router)


@app.get("/missing/{field}", name="missing_movies")
async def missing_movies(request: Request, field: str):
    movies = get_movie_titles_missing(field)
    return templates.TemplateResponse(
        "partials/missing_movies.html",
        {"request": request, "field": field, "movies": movies},
    )


@app.get("/missing_tv/{field}", name="missing_tv")
async def missing_tv(request: Request, field: str):
    titles = get_tv_titles_missing(field)
    return templates.TemplateResponse(
        "partials/missing_tv.html",
        {"request": request, "field": field, "titles": titles},
    )


@router.get("/db_search", response_class=HTMLResponse)
async def db_search_form(request: Request):
    return templates.TemplateResponse(
        "db_search.html",
        {"request": request, "now": datetime.now(), "app_env": APP_ENV},
    )


@router.post("/db_search", response_class=HTMLResponse)
async def db_search_results(
    request: Request, title: str = Form(""), year: str = Form("")
):
    title = title.strip()
    year = year.strip()

    params = []
    movie_conditions = []
    series_conditions = []

    if title:
        movie_conditions.append("m.movie_title ILIKE %s")
        series_conditions.append("s.series_name ILIKE %s")
        params.extend([f"%{title}%", f"%{title}%"])

    if year:
        try:
            year_int = int(year)
            movie_conditions.append("mm.release_year = %s")
            series_conditions.append("DATE_PART('year', s.first_air_date) = %s")
            params.extend([year_int, year_int])
        except ValueError:
            return templates.TemplateResponse(
                "db_search_results.html",
                {
                    "request": request,
                    "results": [],
                    "query": {"title": title, "year": year},
                    "error": "Year must be a valid number",
                    "now": datetime.now(),
                },
            )

    movie_where = " AND ".join(movie_conditions) if movie_conditions else "TRUE"
    series_where = " AND ".join(series_conditions) if series_conditions else "TRUE"

    query = f"""
        SELECT m.movie_id AS id, m.movie_title AS title, mm.release_year, 'movie' AS type
        FROM movies m
        JOIN movie_metadata mm ON mm.movie_id = m.movie_id
        WHERE {movie_where}

        UNION

        SELECT s.series_id AS id, s.series_name AS title,
               DATE_PART('year', s.first_air_date) AS release_year, 'tv' AS type
        FROM series s
        WHERE {series_where}
    """

    with dict_cursor() as cursor:
        cursor.execute(query, params)
        results = cursor.fetchall()

    if len(results) == 1:
        return RedirectResponse(
            url=f"/title/{results[0]['type']}/{results[0]['id']}", status_code=303
        )

    return templates.TemplateResponse(
        "db_search_results.html",
        {
            "request": request,
            "app_env": APP_ENV,
            "results": results,
            "query": {"title": title, "year": year},
            "now": datetime.now(),
        },
    )


@router.get("/title/{title_type}/{title_id}", response_class=HTMLResponse)
async def title_detail(request: Request, title_type: str, title_id: int):
    db = get_connection()

    season_map = []
    series_rating = None
    personal_rating = None

    if title_type == "movie":
        diagnostics = wrap_query("get_title_by_id", lambda: [get_title_by_id(title_id)])

        with dict_cursor() as cur:
            cur.execute(
                """
                SELECT rating
                FROM movie_metadata
                WHERE movie_id = %s
            """,
                (title_id,),
            )
            result = cur.fetchone()
            personal_rating = (
                result["rating"] if result and result["rating"] is not None else None
            )

    elif title_type == "tv":
        diagnostics = wrap_query(
            "get_series_by_id", lambda: [get_series_by_id(title_id)]
        )
        title = diagnostics["data"][0] if diagnostics["record_count"] else None

        with db.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT season_id, season_number, air_date, poster_path, season_name, overview
                FROM series_seasons
                WHERE series_id = %s
                ORDER BY season_number
            """,
                (title_id,),
            )
            seasons = cur.fetchall()

            season_map = []
            for season in seasons:
                cur.execute(
                    """
                    SELECT
                        e.episode_id,
                        e.episode_number,
                        e.episode_name,
                        e.overview,
                        e.air_date,
                        m.rating,
                        m.watched_date
                    FROM series_episodes e
                    LEFT JOIN episode_metadata m ON m.episode_id = e.episode_id
                    WHERE e.season_id = %s
                    ORDER BY e.episode_number
                """,
                    (season["season_id"],),
                )
                season["episodes"] = cur.fetchall()

                season["air_date_formatted"] = format_local(
                    season.get("air_date"), "%-d %B %Y"
                )

                for ep in season["episodes"]:
                    ep["air_date_formatted"] = format_local(
                        ep.get("air_date"), "%-d %B %Y"
                    )
                    ep["watched_date_formatted"] = format_local(
                        ep.get("watched_date"), "%-d %B %Y"
                    )

                dates = [
                    ep["air_date"] for ep in season["episodes"] if ep.get("air_date")
                ]
                if dates:
                    season["date_from"] = format_local(min(dates), "%-d %B %Y")
                    season["date_to"] = format_local(max(dates), "%-d %B %Y")
                else:
                    season["date_from"] = season["date_to"] = None

                cur.execute(
                    """
                    SELECT ROUND(AVG(m.rating)::numeric, 2) AS average_rating
                    FROM series_episodes e
                    JOIN episode_metadata m ON m.episode_id = e.episode_id
                    WHERE e.season_id = %s
                """,
                    (season["season_id"],),
                )
                season["average_rating"] = cur.fetchone()["average_rating"]

                season_map.append(season)

            cur.execute(
                """
                SELECT ROUND(AVG(m.rating)::numeric, 2) AS series_average_rating
                FROM series_episodes e
                JOIN episode_metadata m ON m.episode_id = e.episode_id
                WHERE e.series_id = %s
            """,
                (title_id,),
            )
            series_rating = cur.fetchone()["series_average_rating"]

    else:
        return HTMLResponse(content="Invalid title type", status_code=400)

    if title_type == "movie":
        title = diagnostics["data"][0] if diagnostics["record_count"] else None

    cast = get_cast_for_title(title_id, title_type)
    crew = get_crew_for_title(title_id, title_type)
    related_titles = get_related_titles(title_id, title_type)

    return templates.TemplateResponse(
        "title_detail.html",
        {
            "request": request,
            "app_env": APP_ENV,
            "title": title,
            "cast": cast,
            "crew": crew,
            "season_map": season_map,
            "series_rating": series_rating,
            "diagnostics": diagnostics,
            "personal_rating": personal_rating,
            "related_titles": related_titles,
            "now": datetime.now(),
        },
    )


def get_seasons_for_series(series_id: int):
    query = """
        SELECT
            season_id,
            season_number,
            air_date,
            poster_path,
            season_name,
            overview
        FROM series_seasons
        WHERE series_id = %s
        ORDER BY season_number
    """
    with dict_cursor() as cursor:
        cursor.execute(query, (series_id,))
        return cursor.fetchall()


def get_episodes_for_season(season_id: int):
    query = """
        SELECT
            e.episode_id,
            e.season_id,
            e.episode_number,
            e.name,
            e.overview,
            e.air_date,
            m.rating,
            m.watched_date
        FROM series_episodes e
        LEFT JOIN episode_metadata m ON m.episode_id = e.episode_id
        WHERE e.season_id = %s
        ORDER BY e.episode_number
    """
    db = get_connection()
    with db.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(query, (season_id,))
        return cursor.fetchall()


def get_season_episode_map(series_id: int):
    seasons = get_seasons_for_series(series_id)
    for season in seasons:
        season["episodes"] = get_episodes_for_season(season["season_id"])
    return seasons


def get_average_ratings(series_id: int):
    db = get_connection()
    with db.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                s.season_id,
                s.season_number,
                ROUND(AVG(m.rating)::numeric, 2) AS average_rating
            FROM series_seasons s
            JOIN series_episodes e ON e.season_id = s.season_id
            JOIN episode_metadata m ON m.episode_id = e.episode_id
            WHERE s.series_id = %s
            GROUP BY s.season_id, s.season_number
            ORDER BY s.season_number;
        """,
            (series_id,),
        )
        season_ratings = cur.fetchall()

        cur.execute(
            """
            SELECT ROUND(AVG(m.rating)::numeric, 2) AS average_rating
            FROM series_episodes e
            JOIN episode_metadata m ON m.episode_id = e.episode_id
            WHERE e.series_id = %s
        """,
            (series_id,),
        )
        series_rating = cur.fetchone()["average_rating"]

    return season_ratings, series_rating


@app.get("/", response_class=HTMLResponse, name="index")
async def index(request: Request):
    now = datetime.now()
    next_month = (now.replace(day=1) + timedelta(days=32)).replace(day=1)

    current_releases = get_cinema_releases(month=now.month, year=now.year)
    next_month_releases = get_cinema_releases(
        month=next_month.month, year=next_month.year
    )
    tv_releases = get_tv_releases(month=now.month, year=now.year)

    context = {
        **get_stats_context(request),
        "app_env": APP_ENV,
        "app_version": "0.1.0",
        "now": now,
        "next_month": next_month,
        "current_releases": current_releases,
        "next_month_releases": next_month_releases,
        "tv_releases": tv_releases,
    }

    return templates.TemplateResponse("index.html", context)


@router.get("/statistics", response_class=HTMLResponse, name="statistics")
async def statistics(request: Request):
    return templates.TemplateResponse("statistics.html", get_stats_context(request))


@router.get("/uploader", response_class=HTMLResponse, name="uploader")
async def uploader(request: Request):
    return templates.TemplateResponse("uploader.html", get_stats_context(request))


@app.get("/news")
def news_page(request: Request):
    articles = get_all_news(api_key="pub_000738d4a1274d798638038b9633580c")
    return templates.TemplateResponse(
        "news.html", {"request": request, "articles": articles}
    )

def get_stats_context(request: Request):
    stats_blob = stats.get_all_stats()

    # Convert last_update string → datetime
    raw_last_update = stats_blob.get("last_update")
    if isinstance(raw_last_update, str):
        try:
            last_update = datetime.fromisoformat(raw_last_update.replace("Z", "+00:00"))
        except Exception:
            last_update = None
    else:
        last_update = raw_last_update

    return {
        "request": request,
        "now": datetime.now(),
        "app_env": APP_ENV,
        "app_version": "0.1.0",

        "top_fields": stats_blob["top_fields"],
        "movie_count": stats_blob["movie_count"],
        "series_count": stats_blob["series_count"],
        "last_update": last_update,
        "most_updated_title": stats_blob["most_updated_title"],
        "orphaned_logs": stats_blob["orphaned_logs"],
        "movies_missing_fields": stats_blob["movies_missing_fields"],  # <-- FIX
        "series_missing_fields": stats_blob["series_missing_fields"],  # <-- you will need this too
        "freshness": stats_blob["freshness"],  # <-- and this
        "stats": stats_blob,
    }


app.include_router(router)


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


@app.api_route("/search_person", methods=["GET", "POST"], response_class=HTMLResponse)
async def search_person(request: Request):
    if request.method == "POST":
        form = await request.form()
        name = form.get("person_name")
        people = search_person_tmdb(name)
    else:
        name = request.query_params.get("person_name")
        people = search_person_tmdb(name) if name else []

    return templates.TemplateResponse(
        "person_results.html",
        {
            "request": request,
            "people": people,
            "now": datetime.now(),
        },
    )


@app.post("/tmdb_search", response_class=HTMLResponse)
async def tmdb_search(request: Request):
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
        "tmdb_search_results.html",
        {"request": request, "results": annotated_results, "now": datetime.now()},
    )


def annotate_results_with_db_status(results):
    with dict_cursor() as cur:
        annotated = []
        for result in results:
            annotated.append(annotate_result(cur, result))
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
        last_updated = row["last_updated"] if row else None

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
            "upload_status": "Upload complete",
            "raw_changes": changes,
            "now": datetime.now(),
        },
    )


@app.get("/bulk-upload", response_class=HTMLResponse)
async def bulk_upload_form(request: Request):
    return templates.TemplateResponse(
        "bulk_upload.html",
        {
            "request": request,
            "now": datetime.now(),
        },
    )


@app.post("/bulk-upload", response_class=HTMLResponse)
async def bulk_upload(
    request: Request, media_type: str = Form(...), id_list: str = Form(...)
):
    conn = None
    grouped_results = []
    now = datetime.now()

    try:
        conn = get_connection()
        ids = [i.strip() for i in id_list.replace(",", "\n").splitlines() if i.strip()]
        ids = ids[:100]

        for tmdb_id in ids:
            try:
                previous_max = get_previous_log_timestamp(conn, tmdb_id, media_type)
                if previous_max is None:
                    previous_max = datetime.min

                content_id, base_message = process_media_upload(
                    conn, tmdb_id, media_type
                )

                title = ""
                filtered = []
                message = ""

                if content_id:
                    changes = fetch_new_update_logs(
                        conn, content_id, media_type, previous_max
                    )
                    filtered = filter_changes(changes)

                    if filtered:
                        first = filtered[0]
                        title = (
                            first.get("title")
                            or first.get("movie_title")
                            or first.get("series_title")
                            or ""
                        )

                    if not title:
                        tmdb_data = get_tmdb_data(tmdb_id, media_type)
                        title = (
                            tmdb_data.get("title")
                            or tmdb_data.get("name")
                            or "Unknown Title"
                        )

                    message = (
                        base_message
                        if filtered
                        else f"{base_message} No changes needed — already up-to-date."
                    )
                else:
                    message = (
                        f"{base_message} No movie ID returned — upload may have failed."
                    )

                grouped_results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "title": title or "Unknown Title",
                        "message": message,
                        "changes": filtered,
                    }
                )

            except Exception as e:
                grouped_results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "title": "Error",
                        "message": f"❌ Error processing ID {tmdb_id}: {str(e)}",
                        "changes": [],
                    }
                )
                traceback.print_exc()

    finally:
        if conn:
            conn.close()

    return templates.TemplateResponse(
        "bulk_result.html",
        {
            "request": request,
            "results": grouped_results,
            "media_type": media_type,
            "upload_status": "Bulk upload complete",
            "now": now,
        },
    )


TMDB_BASE = "https://api.themoviedb.org/3"

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


def get_english_language_info(movie_id: int) -> Dict:
    url = f"{TMDB_BASE}/movie/{movie_id}"
    params = {"api_key": TMDB_API_KEY}
    try:
        response = requests.get(url, params=params, timeout=5)
        data = response.json()
        if data.get("original_language") == "en":
            return {
                "title": data.get("title", ""),
                "release_date": data.get("release_date", ""),
                "overview": data.get("overview", ""),
                "language": data.get("original_language", ""),
            }
    except Exception as e:
        print(f"Error fetching movie info for {movie_id}: {e}")
    return {}


def format_local(dt, fmt="%d %b %Y, %H:%M"):
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except ValueError:
            return "Unknown"
    elif isinstance(dt, date) and not isinstance(dt, datetime):
        dt = datetime.combine(dt, datetime.min.time())
    elif not isinstance(dt, datetime):
        return "Unknown"

    return dt.astimezone(ZoneInfo("Europe/London")).strftime(fmt)


BASE_URL = "https://api.themoviedb.org/3"


def get_imdb_id(credit):
    if credit["media_type"] == "movie":
        url = f"{BASE_URL}/movie/{credit['id']}?api_key={TMDB_API_KEY}"
    elif credit["media_type"] == "tv":
        url = f"{BASE_URL}/tv/{credit['id']}/external_ids?api_key={TMDB_API_KEY}"
    else:
        return None

    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json().get("imdb_id")
    return None
