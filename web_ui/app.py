import os
import sys
import requests
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv
from starlette.routing import Route
from db.connection import get_connection

# TMDB API key
from config.settings import TMDB_API_KEY
from uploader.movie_uploader import insert_or_update_movie_data
from uploader.tv_uploader import insert_or_update_series_data
from tmdb.movie_api import get_movie_data
from tmdb.tv_api import fetch_series

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Load environment variables
load_dotenv()

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="web_ui/templates")


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

    # Optional: sort by popularity
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

    # Connect to DB and check existence
    conn = get_connection()
    annotated_results = []
    try:
        with conn.cursor() as cur:
            for result in results:
                tmdb_id = result.get("id")
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM movies WHERE id = %s
                    );
                """,
                    (tmdb_id,),
                )
                exists = cur.fetchone()[0]
                result["exists"] = exists
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

        if media_type == "movie":
            movie_data = get_movie_data(tmdb_id)
            insert_or_update_movie_data(conn, movie_data)
            message = f"✅ Movie '{movie_data.get('title')}' processed successfully."
            movie_id = movie_data["id"]
        elif media_type == "tv":
            series_data = fetch_series(tmdb_id)
            insert_or_update_series_data(conn, series_data)
            message = (
                f"✅ TV Series '{series_data.get('name')}' processed successfully."
            )
            movie_id = series_data["id"]
        else:
            message = "❌ Invalid media type selected."
            movie_id = None

        changes = []
        if movie_id:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT timestamp, change_type, field_name, old_value, new_value
                    FROM update_logs
                    WHERE movie_id = %s
                    ORDER BY timestamp DESC;
                """,
                    (movie_id,),
                )
                logs = cur.fetchall()

            for log in logs:
                timestamp, change_type, field, old, new = log
                changes.append(
                    {
                        "timestamp": timestamp,
                        "change_type": change_type,
                        "field": field,
                        "old_value": old,
                        "new_value": new,
                    }
                )
        conn.close()
    except Exception as e:
        message = f"❌ Error occurred: {str(e)}"
        changes = []

    return templates.TemplateResponse(
        "result.html", {"request": request, "message": message, "changes": changes}
    )
