from datetime import datetime
from db.logger import log_update
from tmdb.tv_api import fetch_series, fetch_season
import requests

# 🔍 Fetch IMDb ID for a TV series using TMDB's external_ids endpoint
def fetch_imdb_id(tv_id, api_key):
    url = f"https://api.themoviedb.org/3/tv/{tv_id}"
    params = {
        "api_key": api_key,
        "append_to_response": "external_ids"
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("external_ids", {}).get("imdb_id")
    except Exception as e:
        print(f"Failed to fetch IMDb ID for TV ID {tv_id}: {e}")
        return None

# 👤 Ensure a person (cast/crew) exists in the people table before linking th
def ensure_person_exists(cur, person):
    cur.execute("SELECT 1 FROM people WHERE person_id = %s;", (person["id"],))
    if not cur.fetchone():
        print(f"🧑 Adding person '{person['name']}' to people table")
        cur.execute("""
            INSERT INTO people (person_id, name, profile_path, popularity, lastupdated)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT DO NOTHING;
        """, (
            person["id"],
            person["name"],
            person.get("profile_path"),
            person.get("popularity", 0)
        ))

# 🚀 Main entry point to insert or update a TV series and its related data
def insert_or_update_series_data(conn, series, tmdb_api_key):
    # Fetch IMDb ID early so it's available for insert/update logic
    series["imdb_id"] = fetch_imdb_id(series["id"], tmdb_api_key)

    with conn.cursor() as cur:
        # Check if the series already exists in the database
        cur.execute("SELECT * FROM series WHERE series_id = %s;", (series["id"],))
        existing = cur.fetchone()

        # Update or insert based on existence
        if existing:
            update_series_data(conn, series, existing)
        else:
            insert_series_data(conn, series, cur)

        series_id = series["id"]
        series_title = series["name"]

        # 🎭 Insert cast members (up to 30 for performance)
        for cast in series.get("credits", {}).get("cast", [])[:30]:
            ensure_person_exists(cur, cast)
            cur.execute("SELECT 1 FROM series_cast WHERE series_id = %s AND person_id = %s;", (series_id, cast["id"]))
            if not cur.fetchone():
                print(f"🎭 Added cast member '{cast['name']}' as '{cast.get('character')}' to series '{series_title}'")
                log_update(cur, series_id, series_title, "cast_added", "cast", None, cast["name"])
                cur.execute("""
                    INSERT INTO series_cast (
                        series_id, person_id, cast_order, character_name, lastupdated
                    )
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT DO NOTHING;
                """, (
                    series_id,
                    cast["id"],
                    cast.get("order", 0),
                    cast.get("character")
                ))

        # 🎬 Insert crew members
        for crew in series.get("credits", {}).get("crew", []):
            ensure_person_exists(cur, crew)
            cur.execute("SELECT 1 FROM series_crew WHERE series_id = %s AND person_id = %s;", (series_id, crew["id"]))
            if not cur.fetchone():
                print(f"🎬 Added crew member '{crew['name']}' ({crew.get('job')}) to series '{series_title}'")
                log_update(cur, series_id, series_title, "crew_added", "crew", None, crew["name"])
                cur.execute("""
                    INSERT INTO series_crew (
                        series_id, person_id, job, lastupdated
                    )
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT DO NOTHING;
                """, (
                    series_id,
                    crew["id"],
                    crew.get("job")
                ))

        # 📺 Insert seasons and episodes
        for s in series.get("seasons", []):
            season_data = fetch_season(series_id, s["season_number"])
            cur.execute("SELECT 1 FROM season WHERE season_id = %s;", (season_data["id"],))
            if not cur.fetchone():
                print(f"📺 Added season '{season_data.get('name')}' to series '{series_title}'")
                log_update(cur, series_id, series_title, "season_added", "season", None, season_data.get("name"))
                cur.execute("""
                    INSERT INTO season (
                        season_id, series_id, season_number, air_date,
                        lastupdated, poster_path, name, overview
                    )
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (
                    season_data["id"],
                    series_id,
                    season_data["season_number"],
                    season_data.get("air_date"),
                    season_data.get("poster_path"),
                    season_data.get("name"),
                    season_data.get("overview")
                ))

            # 🎞️ Insert episodes for each season
            for ep in season_data.get("episodes", []):
                cur.execute("SELECT 1 FROM episode WHERE episode_id = %s;", (ep["id"],))
                if not cur.fetchone():
                    print(f"🎞️ Added episode '{ep.get('name')}' to season {season_data['season_number']} of series '{series_title}'")
                    log_update(cur, series_id, series_title, "episode_added", "episode", None, ep.get("name"))
                    cur.execute("""
                        INSERT INTO episode (
                            episode_id, season_id, series_id, episode_number,
                            vote_average, vote_count, air_date, runtime,
                            name, overview, still_path, lastupdated
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT DO NOTHING;
                    """, (
                        ep["id"],
                        season_data["id"],
                        series_id,
                        ep.get("episode_number"),
                        ep.get("vote_average"),
                        ep.get("vote_count"),
                        ep.get("air_date"),
                        ep.get("runtime"),
                        ep.get("name"),
                        ep.get("overview"),
                        ep.get("still_path")
                    ))

        # ✅ Final commit after all inserts
        conn.commit()

# 🔄 Update existing series record and log field-level changes
def update_series_data(conn, series, existing):
    with conn.cursor() as cur:
        fields = [
            "name", "overview", "first_air_date", "last_air_date", "number_of_seasons",
            "number_of_episodes", "popularity", "vote_average", "vote_count", "poster_path",
            "backdrop_path", "original_language", "status", "homepage", "imdb_id"
        ]

        updates, values, changed_fields = [], [], []

        new_data = [
            series["name"], series.get("overview"), series.get("first_air_date"), series.get("last_air_date"),
            series.get("number_of_seasons"), series.get("number_of_episodes"), series.get("popularity"),
            series.get("vote_average"), series.get("vote_count"), series.get("poster_path"),
            series.get("backdrop_path"), series.get("original_language"), series.get("status"),
            series.get("homepage"), series.get("imdb_id")
        ]

        # Compare each field and log changes
        for i, (field, new_value) in enumerate(zip(fields, new_data)):
            old_value = existing[i + 1]
            if field in ["first_air_date", "last_air_date"] and isinstance(new_value, str):
                try:
                    new_value = datetime.strptime(new_value.strip(), "%Y-%m-%d").date()
                except ValueError:
                    pass
            if new_value != old_value:
                updates.append(f"{field} = %s")
                values.append(new_value)
                changed_fields.append((field, old_value, new_value))
                log_update(cur, series["id"], series["name"], "field_change", field, old_value, new_value)

        # Apply updates if any fields changed
        if updates:
            updates.append("lastupdated = CURRENT_TIMESTAMP")
            values.append(series["id"])
            query = f"UPDATE series SET {', '.join(updates)} WHERE series_id = %s;"
            cur.execute(query, values)
            conn.commit()
            print(f"🔄 Updated series: {series['name']}")
            for field, old, new in changed_fields:
                print(f" - {field}: '{old}' ➡️ '{new}'")
        else:
            print(f"✅ No changes for series: {series['name']}")

# 🆕 Insert new series record and log each field as an insert
def insert_series_data(conn, series, cur):
    query = """
    INSERT INTO series (series_id, name, overview, first_air_date, last_air_date,
    number_of_seasons, number_of_episodes, popularity, vote_average, vote_count,
    poster_path, backdrop_path, original_language, status, homepage, imdb_id, lastupdated)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT DO NOTHING;
    """
    values = (
        series["id"], series["name"], series.get("overview"), series.get("first_air_date"),
        series.get("last_air_date"), series.get("number_of_seasons"), series.get("number_of_episodes"),
        series.get("popularity"), series.get("vote_average"), series.get("vote_count"),
        series.get("poster_path"), series.get("backdrop_path"), series.get("original_language"),
        series.get("status"), series.get("homepage"), series.get("imdb_id")
    )
    cur.execute(query, values)
    conn.commit()
    print(f"✅ Inserted series: {series['name']}")
