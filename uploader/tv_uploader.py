from datetime import datetime
from db.logger import log_update
from tmdb.tv_api import fetch_series, fetch_season

DRY_RUN = True  # Set to False to enable database writes

def insert_or_update_series_data(conn, series):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM series WHERE series_id = %s;", (series["id"],))
        existing = cur.fetchone()

        if existing:
            update_series_data(conn, series, existing)
        else:
            insert_series_data(conn, series)

        series_id = series["id"]
        series_title = series["name"]

        for cast in series.get("credits", {}).get("cast", [])[:30]:
            cur.execute("SELECT 1 FROM series_cast WHERE series_id = %s AND person_id = %s;", (series_id, cast["id"]))
            if not cur.fetchone():
                print(f"🎭 Added cast member '{cast['name']}' as '{cast.get('character')}' to series '{series_title}'")
                log_update(cur, series_id, series_title, "cast_added", "cast", None, cast["name"])

        for crew in series.get("credits", {}).get("crew", []):
            cur.execute("SELECT 1 FROM series_crew WHERE series_id = %s AND person_id = %s;", (series_id, crew["id"]))
            if not cur.fetchone():
                print(f"🎬 Added crew member '{crew['name']}' ({crew.get('job')}) to series '{series_title}'")
                log_update(cur, series_id, series_title, "crew_added", "crew", None, crew["name"])

        for s in series.get("seasons", []):
            season_data = fetch_season(series_id, s["season_number"])
            cur.execute("SELECT 1 FROM season WHERE season_id = %s;", (season_data["id"],))
            if not cur.fetchone():
                print(f"📺 Added season '{season_data.get('name')}' to series '{series_title}'")
                log_update(cur, series_id, series_title, "season_added", "season", None, season_data.get("name"))

            for ep in season_data.get("episodes", []):
                cur.execute("SELECT 1 FROM episode WHERE episode_id = %s;", (ep["id"],))
                if not cur.fetchone():
                    print(f"🎞️ Added episode '{ep.get('name')}' to season {season_data['season_number']} of series '{series_title}'")
                    log_update(cur, series_id, series_title, "episode_added", "episode", None, ep.get("name"))

        if not DRY_RUN:
            conn.commit()

def update_series_data(conn, series, existing):
    with conn.cursor() as cur:
        fields = [
            "name", "overview", "first_air_date", "last_air_date", "number_of_seasons",
            "number_of_episodes", "popularity", "vote_average", "vote_count", "poster_path",
            "backdrop_path", "original_language", "status", "homepage"
        ]
        updates, values, changed_fields = [], [], []

        new_data = [
            series["name"], series.get("overview"), series.get("first_air_date"), series.get("last_air_date"),
            series.get("number_of_seasons"), series.get("number_of_episodes"), series.get("popularity"),
            series.get("vote_average"), series.get("vote_count"), series.get("poster_path"),
            series.get("backdrop_path"), series.get("original_language"), series.get("status"),
            series.get("homepage")
        ]

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

        if updates:
            updates.append("lastupdated = CURRENT_TIMESTAMP")
            values.append(series["id"])
            query = f"UPDATE series SET {', '.join(updates)} WHERE series_id = %s;"
            if DRY_RUN:
                print(f"[DRY-RUN] Would execute: {query} with {values}")
            else:
                cur.execute(query, values)
                conn.commit()
            print(f"🔄 Updated series: {series['name']}")
            for field, old, new in changed_fields:
                print(f" - {field}: '{old}' ➡️ '{new}'")
        else:
            print(f"✅ No changes for series: {series['name']}")

def insert_series_data(conn, series):
    with conn.cursor() as cur:
        query = """
        INSERT INTO series (series_id, name, overview, first_air_date, last_air_date,
        number_of_seasons, number_of_episodes, popularity, vote_average, vote_count,
        poster_path, backdrop_path, original_language, status, homepage, lastupdated)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT DO NOTHING;
        """
        values = (
            series["id"], series["name"], series.get("overview"), series.get("first_air_date"),
            series.get("last_air_date"), series.get("number_of_seasons"), series.get("number_of_episodes"),
            series.get("popularity"), series.get("vote_average"), series.get("vote_count"),
            series.get("poster_path"), series.get("backdrop_path"), series.get("original_language"),
            series.get("status"), series.get("homepage")
        )
        if DRY_RUN:
            print(f"[DRY-RUN] Would execute: {query} with {values}")
        else:
            cur.execute(query, values)
        if not DRY_RUN:
            conn.commit()
        print(f"✅ Inserted series: {series['name']}")
