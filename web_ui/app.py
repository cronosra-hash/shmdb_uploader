import os
import sys
import requests
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv
from db.connection import get_connection

# TMDB API key
from config.settings import TMDB_API_KEY
from uploader.movie_uploader import insert_or_update_movie_data
from uploader.tv_uploader import insert_or_update_series_data
from tmdb.movie_api import get_movie_data
from tmdb.tv_api import fetch_series

from datetime import datetime

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Load environment variables
load_dotenv()

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="web_ui/templates")

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


@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


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
            insert_or_update_series_data(conn, series_data)
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
