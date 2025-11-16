from datetime import datetime, date
from db.logger import log_update
from utils import parse_date
from utils.logging import safe_json_context
import requests
import traceback
from psycopg2 import sql
import json


def update_series_data(conn, series, media_type="tv", verbose=False):
    """
    Updates an existing series record in the database if any fields have changed.
    Uses schema-aware comparison and logs each change for auditing.
    """
    series_id = series.get("series_id")
    series_title = series.get("title")

    with conn.cursor() as cur:
        cur.execute("SELECT * FROM series WHERE series_id = %s;", (series_id,))
        existing_row = cur.fetchone()

        if not existing_row:
            print(f"⚠️ Series ID {series_id} not found in database.")
            return

        column_names = [desc[0] for desc in cur.description]
        fields_dict = extract_series_fields(series)

        updates, values, changed_fields = compare_fields(
            existing_row, column_names, fields_dict, verbose=verbose
        )

        if updates:
            updates.append("last_updated = CURRENT_TIMESTAMP")
            values.append(series_id)

            query = sql.SQL("UPDATE series SET {} WHERE series_id = %s;").format(
                sql.SQL(", ").join(map(sql.SQL, updates))
            )
            cur.execute(query, values)
            conn.commit()

        print(f"🔄 Updated Series: {series_title}")

        for field, old, new in changed_fields:
            print(f" - {field}: '{old}' ➡️ '{new}'")

            context = safe_json_context(
                {
                    "action": "update",
                    "field": field,
                    "previous": old,
                    "current": new,
                    "source": "series_update_pipeline",
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

            log_update(
                cur,
                content_id=series_id,
                content_title=series_title,
                content_type=media_type,
                update_type="field_updated",
                field_name=field,
                previous_value=old,
                current_value=new,
                context=context,
                source="backend_script",
                timestamp=datetime.utcnow(),
            )
        else:
            print(f"✅ No changes for series: {series_title}")

    return fields_dict


def compare_fields(
    existing_row, column_names, fields, verbose=False, content_type="tv"
):
    updates = []
    values = []
    changed_fields = []

    for field, new in fields.items():
        if field not in column_names:
            if verbose:
                print(f"⚠️ Skipping unknown field: {field}")
            continue

        idx = column_names.index(field)
        old = existing_row[idx]

        # Normalize types
        if isinstance(old, (int, float)) and isinstance(new, str):
            try:
                new = float(new) if "." in new else int(new)
            except ValueError:
                pass

        # Normalize dates
        if "date" in field and isinstance(new, str):
            new = new.strip()
            if new == "":
                new = None

        # Skip if both are None or empty
        if (old is None and new is None) or (
            str(old).strip() == "" and str(new).strip() == ""
        ):
            continue

        # Float comparison with tolerance
        if isinstance(old, float) and isinstance(new, float):
            if abs(old - new) < 0.0001:
                continue

        # Skip if values are equal
        if str(old) == str(new):
            continue

        # Field has changed
        updates.append(f"{field} = %s")
        values.append(new)
        changed_fields.append((field, old, new))

        if verbose:
            print(f"🔄 [{content_type}] Field changed: {field}: '{old}' ➡️ '{new}'")

    return updates, values, changed_fields


def extract_series_fields(series):
    return {
        "series_name": series.get("title") or series.get("name"),
        "overview": series.get("overview"),
        "first_air_date": parse_date(series.get("first_air_date")),
        "last_air_date": parse_date(series.get("last_air_date")),
        "number_of_seasons": series.get("number_of_seasons"),
        "number_of_episodes": series.get("number_of_episodes"),
        "popularity": series.get("popularity"),
        "vote_average": series.get("vote_average"),
        "vote_count": series.get("vote_count"),
        "poster_path": series.get("poster_path"),
        "backdrop_path": series.get("backdrop_path"),
        "original_language": series.get("original_language"),
        "status": series.get("status"),
        "homepage": series.get("homepage"),
        "imdb_id": series.get("imdb_id"),
    }


def insert_series_data(conn, series, media_type="tv", verbose=False):
    """
    Inserts or updates a series record in the database.
    Logs each field change or insertion for auditing.
    """
    fields = extract_series_fields(series)
    series_id = series.get("series_id") or series.get("id")
    series_title = series.get("title") or fields.get("series_name")

    with conn.cursor() as cur:
        # Check if series already exists
        cur.execute("SELECT * FROM series WHERE series_id = %s;", (series_id,))
        existing_row = cur.fetchone()

        if existing_row:
            column_names = [desc[0] for desc in cur.description]
            updates, values, changed_fields = compare_fields(
                existing_row, column_names, fields, verbose=verbose
            )

            if updates:
                updates.append("last_updated = NOW()")
                values.append(series_id)

                query = sql.SQL("UPDATE series SET {} WHERE series_id = %s;").format(
                    sql.SQL(", ").join(map(sql.SQL, updates))
                )
                cur.execute(query, values)
                conn.commit()

            print(f"🔄 Updated Series: {series_title}")

            for field, old, new in changed_fields:
                print(f" - {field}: '{old}' ➡️ '{new}'")

                context = safe_json_context(
                    {
                        "action": "update",
                        "field": field,
                        "previous": old,
                        "current": new,
                        "source": "series_update_pipeline",
                        "timestamp": datetime.utcnow().isoformat(),
                    }
                )

                log_update(
                    cur,
                    content_id=series_id,
                    content_title=series_title,
                    content_type=media_type,
                    update_type="field_updated",
                    field_name=field,
                    previous_value=old,
                    current_value=new,
                    context=context,
                    source="backend_script",
                    timestamp=datetime.utcnow(),
                )
            else:
                print(f"✅ No changes for series: {series_title}")
        else:
            # Insert new series
            query = """
            INSERT INTO series (
                series_id, series_name, overview, first_air_date, last_air_date,
                number_of_seasons, number_of_episodes, popularity, vote_average, vote_count,
                poster_path, backdrop_path, original_language, status, homepage, imdb_id, last_updated
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT DO NOTHING;
            """

            values = [
                series_id,
                fields.get("series_name"),
                fields.get("overview"),
                fields.get("first_air_date"),
                fields.get("last_air_date"),
                fields.get("number_of_seasons"),
                fields.get("number_of_episodes"),
                fields.get("popularity"),
                fields.get("vote_average"),
                fields.get("vote_count"),
                fields.get("poster_path"),
                fields.get("backdrop_path"),
                fields.get("original_language"),
                fields.get("status"),
                fields.get("homepage"),
                fields.get("imdb_id"),
            ]

            cur.execute(query, values)
            conn.commit()

            print(f"🆕 Inserted series: {series_title}")

            for field, value in fields.items():
                if value is not None:
                    context = json.dumps(
                        {
                            "action": "insert",
                            "field": field,
                            "value": value,
                            "source": "series_insert_pipeline",
                            "timestamp": datetime.utcnow().isoformat(),
                        }
                    )

                    log_update(
                        cur,
                        content_id=series_id,
                        content_title=series_title,
                        content_type=media_type,
                        update_type="field_inserted",
                        field_name=field,
                        previous_value=None,
                        current_value=value,
                        context=context,
                        source="backend_script",
                        timestamp=datetime.utcnow(),
                    )

    return fields


def fetch_imdb_id(tv_id, api_key):
    """
    Fetches IMDb ID for a TV series using TMDB's external_ids endpoint.
    """
    url = f"https://api.themoviedb.org/3/tv/{tv_id}"
    params = {"api_key": api_key, "append_to_response": "external_ids"}

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        imdb_id = data.get("external_ids", {}).get("imdb_id")

        if imdb_id:
            print(f"🔗 IMDb ID for TV ID {tv_id}: {imdb_id}")
        else:
            print(f"⚠️ No IMDb ID found for TV ID {tv_id}")

        return imdb_id

    except requests.RequestException as e:
        print(f"❌ TMDB request failed for TV ID {tv_id}: {e}")
    except Exception as e:
        print(f"❌ Unexpected error fetching IMDb ID for TV ID {tv_id}: {e}")

    return None


def ensure_person_exists(cur, person):
    """
    Ensures a person exists in the people table before linking them to cast/crew.
    """
    person_id = person.get("id")
    name = person.get("name")

    if not person_id or not name:
        print(f"⚠️ Invalid person payload: {person}")
        return

    cur.execute("SELECT 1 FROM people WHERE person_id = %s;", (person_id,))
    if not cur.fetchone():
        print(f"🧑 Adding person '{name}' to people table")
        cur.execute(
            """
            INSERT INTO people (person_id, name, profile_path, popularity, last_updated)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT DO NOTHING;
        """,
            (
                person_id,
                name,
                person.get("profile_path"),
                float(person.get("popularity", 0)),
            ),
        )


def insert_or_update_series_data(conn, series, tmdb_api_key):
    """
    Inserts or updates series data in the database, including genres, cast, and crew.
    """
    series = normalize_series_payload(series)
    series_id = series.get("series_id") or series.get("id")
    series["series_id"] = series_id

    if not series_id:
        print("❌ No series ID found in payload — skipping.")
        return

    # Fetch IMDb ID early
    series["imdb_id"] = fetch_imdb_id(series_id, tmdb_api_key)

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM series WHERE series_id = %s;", (series_id,))
            exists = cur.fetchone()

        if exists:
            print(f"🔄 Updating series_id={series_id}")
            update_series_data(conn, series)

        else:
            print(f"🆕 Inserting series_id={series_id}")
            insert_series_data(conn, series)

        with conn.cursor() as cur:
            insert_series_genres(cur, series_id, series)
            insert_series_cast(cur, series_id, series)
            insert_series_crew(cur, series_id, series)
            insert_series_companies(cur, series_id, series)
            insert_series_languages(cur, series_id, series)
            insert_series_countries(cur, series_id, series)
            sync_series_seasons(cur, series)
            sync_series_episodes(cur, series_id, series)

        conn.commit()
        print(f"✅ Series ID {series_id} processed successfully.")

    except Exception as e:
        print(f"❌ Failed to insert/update series_id={series_id}: {e}")
        traceback.print_exc()
        conn.rollback()


def normalize_series_payload(series):
    """
    Cleans and standardizes the incoming series payload for database insertion.
    Ensures consistent keys, default values, and type safety.
    """
    if not series:
        return {}

    # Rename keys for consistency
    series["series_id"] = series.get("id")
    series["title"] = series.get("name") or series.get("original_name")
    series["release_date"] = series.get("first_air_date")
    series["content_type"] = "series"

    # Ensure genres is a list of dicts with id + name
    series["genres"] = series.get("genres", [])
    if isinstance(series["genres"], list):
        series["genres"] = [
            {"id": g.get("id"), "name": g.get("name")}
            for g in series["genres"]
            if g.get("id") and g.get("name")
        ]

    # Credits fallback
    series["credits"] = series.get("credits", {})
    series["credits"]["cast"] = series["credits"].get("cast", [])
    series["credits"]["crew"] = series["credits"].get("crew", [])

    # Optional metadata
    series["overview"] = series.get("overview", "")
    series["poster_path"] = series.get("poster_path")
    series["backdrop_path"] = series.get("backdrop_path")
    series["popularity"] = float(series.get("popularity", 0))
    series["vote_average"] = float(series.get("vote_average", 0))
    series["vote_count"] = int(series.get("vote_count", 0))

    # Normalize nested seasons if present (stub for now)
    series["seasons"] = series.get("seasons", [])

    return series


def insert_series_cast(cur, series_id, series):
    cast_list = series.get("aggregate_credits", {}).get("cast", [])[:30]

    for cast in cast_list:
        ensure_person_exists(cur, cast)

        episode_count = cast.get("total_episode_count", 1)
        character_name = None
        if cast.get("roles"):
            character_name = cast["roles"][0].get("character")

        # Try insert, but return whether it was new or updated
        cur.execute(
            """
            INSERT INTO series_cast (
                series_id, person_id, cast_order, character_name, episode_count, last_updated
            )
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (series_id, person_id) DO UPDATE
            SET episode_count = EXCLUDED.episode_count,
                character_name = COALESCE(EXCLUDED.character_name, series_cast.character_name),
                last_updated = CURRENT_TIMESTAMP
            RETURNING (xmax = 0) AS inserted;
            """,
            (
                series_id,
                cast["id"],
                cast.get("order", 0),
                character_name,
                episode_count,
            ),
        )

        # Postgres trick: xmax=0 means it was a fresh insert, not an update
        inserted = cur.fetchone()[0]

        if inserted:
            context = json.dumps(
                {
                    "action": "link",
                    "entity": "cast",
                    "cast_name": cast["name"],
                    "source": "cast_link_pipeline",
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

            log_update(
                cur,
                content_id=series_id,
                content_title=series["name"],
                content_type="tv",
                update_type="cast_added",
                field_name="cast",
                previous_value=None,
                current_value=cast["name"],
                context=context,
                source="backend_script",
                timestamp=datetime.utcnow(),
            )

            print(f"📺 Cast '{cast['name']}' ({episode_count} episodes) linked to series '{series['name']}'")
        else:
            print(f"↔️ Cast '{cast['name']}' already linked, updated episode_count={episode_count}")


from datetime import datetime
import json

def insert_series_crew(cur, series_id, series):
    crew_list = series.get("aggregate_credits", {}).get("crew", [])
    if not crew_list:
        return

    for crew in crew_list:
        ensure_person_exists(cur, crew)

        department = crew.get("department")

        # Extract job(s) correctly from jobs array
        jobs = []
        if crew.get("jobs"):
            jobs = [j.get("job") for j in crew["jobs"] if j.get("job")]
        if not jobs:
            jobs = ["Unknown"]

        for job in jobs:
            # Check if this crew entry already exists
            cur.execute(
                """
                SELECT 1 FROM series_crew
                WHERE series_id = %s AND person_id = %s AND job = %s;
                """,
                (series_id, crew["id"], job),
            )

            if not cur.fetchone():
                # Insert new crew record
                cur.execute(
                    """
                    INSERT INTO series_crew (
                        series_id, person_id, department, job, last_updated
                    )
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT DO NOTHING;
                    """,
                    (series_id, crew["id"], department, job),
                )

                # Build logging context
                context = json.dumps(
                    {
                        "action": "link",
                        "entity": "crew",
                        "crew_name": crew["name"],
                        "job": job,
                        "source": "crew_link_pipeline",
                        "timestamp": datetime.utcnow().isoformat(),
                    }
                )

                # Log the update
                log_update(
                    cur,
                    content_id=series_id,
                    content_title=series["name"],
                    content_type="tv",
                    update_type="crew_added",
                    field_name="crew",
                    previous_value=None,
                    current_value=f"{crew['name']} ({job})",
                    context=context,
                    source="backend_script",
                    timestamp=datetime.utcnow(),
                )

                print(f"📺 Crew '{crew['name']}' ({department} / {job}) linked to series '{series['name']}'")
            else:
                # Optional: log or print updates when already present
                print(f"↔️ Crew '{crew['name']}' ({job}) already linked to series '{series['name']}'")


def insert_series_genres(cur, series_id, series):
    genres = series.get("genres", [])

    for genre in genres:
        genre_id = genre.get("id")
        genre_name = genre.get("name")

        if not genre_id or not genre_name:
            print(f"⚠️ Skipping genre with missing ID or name: {genre}")
            continue

        # Ensure genre exists in the genres table
        cur.execute(
            """
            INSERT INTO genres (genre_id, genre_name)
            VALUES (%s, %s)
            ON CONFLICT (genre_id) DO NOTHING;
            """,
            (genre_id, genre_name),
        )

        # Link genre to series if not already linked
        cur.execute(
            """
            INSERT INTO series_genres (series_id, genre_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING;
            """,
            (series_id, genre_id),
        )

        if cur.rowcount > 0:
            # Log only if the link was newly created
            context = json.dumps({
                "action": "link",
                "entity": "series_genre",
                "genre_name": genre_name,
                "source": "genre_link_pipeline",
                "timestamp": datetime.utcnow().isoformat(),
            })

            try:
                log_update(
                    cur,
                    content_id=series_id,
                    content_title=series["name"],
                    content_type="tv",
                    update_type="genre_added",
                    field_name="genre",
                    previous_value=None,
                    current_value=genre_name,
                    context=context,
                    source="backend_script",
                    timestamp=datetime.utcnow(),
                )
                print(f"📺 Genre '{genre_name}' linked to series '{series['name']}'")
            except Exception as e:
                print(f"❌ Failed to log genre link for '{genre_name}': {e}")


def insert_series_companies(cur, series_id, series):
    for company in series.get("production_companies", []):
        cur.execute(
            """
            INSERT INTO production_companies (company_id, company_name, logo_path, origin_country)
            VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;
        """,
            (
                company["id"],
                company["name"],
                company.get("logo_path"),
                company.get("origin_country"),
            ),
        )

        cur.execute(
            """
            SELECT 1 FROM series_companies WHERE series_id = %s AND company_id = %s;
        """,
            (series_id, company["id"]),
        )
        if not cur.fetchone():
            cur.execute(
                """
                INSERT INTO series_companies (series_id, company_id) VALUES (%s, %s);
            """,
                (series_id, company["id"]),
            )
            context = json.dumps(
                {
                    "action": "link",
                    "entity": "series_company",
                    "company_name": company["name"],
                    "source": "company_link_pipeline",
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

            log_update(
                cur,
                content_id=series_id,
                content_title=series["name"],
                content_type="tv",
                update_type="company_added",
                field_name="company",
                previous_value=None,
                current_value=company["name"],
                context=context,
                source="backend_script",
                timestamp=datetime.utcnow(),
            )

            print(f"📺 Company '{company['name']}' linked to series '{series['name']}'")


def insert_series_languages(cur, series_id, series):
    for lang in series.get("spoken_languages", []):
        cur.execute(
            """
            INSERT INTO spoken_languages (iso_639_1, language_name)
            VALUES (%s, %s) ON CONFLICT DO NOTHING;
        """,
            (lang["iso_639_1"], lang["name"]),
        )

        cur.execute(
            """
            SELECT 1 FROM series_languages WHERE series_id = %s AND language_code = %s;
        """,
            (series_id, lang["iso_639_1"]),
        )
        if not cur.fetchone():
            cur.execute(
                """
                INSERT INTO series_languages (series_id, language_code) VALUES (%s, %s);
            """,
                (series_id, lang["iso_639_1"]),
            )
            context = json.dumps(
                {
                    "action": "link",
                    "entity": "series_language",
                    "language_name": lang["name"],
                    "source": "language_link_pipeline",
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

            log_update(
                cur,
                content_id=series_id,
                content_title=series["name"],
                content_type="tv",
                update_type="language_added",
                field_name="language",
                previous_value=None,
                current_value=lang["name"],
                context=context,
                source="backend_script",
                timestamp=datetime.utcnow(),
            )

            print(f"📺 Language '{lang['name']}' linked to series '{series['name']}'")


def insert_series_countries(cur, series_id, series):
    for country_code in series.get("origin_country", []):
        if not country_code:
            continue

        # Insert country code only (name may not be available)
        cur.execute(
            """
            INSERT INTO countries (iso_3166_1)
            VALUES (%s) ON CONFLICT DO NOTHING;
            """,
            (country_code,),
        )

        cur.execute(
            """
            SELECT 1 FROM series_countries WHERE series_id = %s AND country_code = %s;
            """,
            (series_id, country_code),
        )
        if not cur.fetchone():
            cur.execute(
                """
                INSERT INTO series_countries (series_id, country_code)
                VALUES (%s, %s);
                """,
                (series_id, country_code),
            )
            context = json.dumps(
                {
                    "action": "link",
                    "entity": "series_country",
                    "country_code": country_code,
                    "source": "country_link_pipeline",
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

            log_update(
                cur,
                content_id=series_id,
                content_title=series["name"],
                content_type="tv",
                update_type="country_added",
                field_name="country",
                previous_value=None,
                current_value=country_code,
                context=context,
                source="backend_script",
                timestamp=datetime.utcnow(),
            )

            print(f"📺 Country '{country_code}' linked to series '{series['name']}'")


def safe_json(val):
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    return val

def normalize(val):
    if isinstance(val, str):
        return val.strip() or None
    if isinstance(val, float):
        return round(val, 3)
    return val or None

def sync_series_episodes(cur, series_id, series):
    print(f"🚨 Syncing episodes for series_id={series_id}, name={series.get('name')}")
    episodes = series.get("episodes", [])
    print(f"📦 Found {len(episodes)} episodes to sync")

    inserted, updated = 0, 0

    for episode in episodes:
        episode_number = episode.get("episode_number")
        season_id = episode.get("season_id")

        if episode_number is None or season_id is None:
            print(f"⚠️ Skipping episode with missing episode_number or season_id")
            continue

        print(f"🔍 Checking episode {episode_number} in season_id={season_id} for series_id={series_id}")

        cur.execute(
            """
            SELECT episode_id, episode_name, overview, air_date, 
                   runtime, still_path, vote_average, vote_count
            FROM series_episodes
            WHERE series_id = %s AND season_id = %s AND episode_number = %s
            """,
            (series_id, season_id, episode_number),
        )
        existing = cur.fetchone()
        print(f"🧾 Fetched existing episode {episode_number}, found={bool(existing)}")

        parsed_air_date = parse_date(episode.get("air_date"))

        fields = {
            "episode_name": episode.get("name"),
            "overview": episode.get("overview"),
            "air_date": parsed_air_date,
            "runtime": episode.get("runtime"),
            "still_path": episode.get("still_path"),
            "vote_average": episode.get("vote_average"),
            "vote_count": episode.get("vote_count"),
        }

        if existing:
            column_names = [
                "episode_id", "episode_name", "overview", "air_date",
                "runtime", "still_path", "vote_average", "vote_count"
            ]
            existing_data = dict(zip(column_names, existing))

            updates = {}
            for field, new_value in fields.items():
                old_value = existing_data.get(field)
                if normalize(new_value) != normalize(old_value):
                    updates[field] = (old_value, new_value)

            if updates:
                set_clause = ", ".join([f"{field} = %s" for field in updates])
                values = [new for _, new in updates.values()]
                values.extend([series_id, season_id, episode_number])

                print(f"🔍 Preparing to update episode {episode_number} in season {season_id}")
                for field, (old, new) in updates.items():
                    print(f"   ↪ {field}: '{old}' → '{new}'")

                cur.execute(
                    f"""
                    UPDATE series_episodes
                    SET {set_clause}, last_updated = CURRENT_TIMESTAMP
                    WHERE series_id = %s AND season_id = %s AND episode_number = %s
                    """,
                    values,
                )
                print(f"🔧 Updated episode {episode_number}, fields: {list(updates.keys())}")

                for field, (old, new) in updates.items():
                    try:
                        context = json.dumps({
                            "action": "update",
                            "entity": "series_episode",
                            "episode_number": episode_number,
                            "field": field,
                            "old": safe_json(old),
                            "new": safe_json(new),
                            "source": "episode_sync_pipeline",
                            "timestamp": datetime.utcnow().isoformat(),
                        })

                        log_update(
                            cur,
                            content_id=series_id,
                            content_title=series["name"],
                            content_type="tv",
                            update_type="episode_updated",
                            field_name=field,
                            previous_value=old,
                            current_value=new,
                            context=context,
                            source="backend_script",
                            timestamp=datetime.utcnow().isoformat(),
                        )
                    except Exception as e:
                        print(f"❌ Failed to log update for field '{field}': {e}")

                updated += 1
                print(f"🔄 Episode {episode_number} updated for series '{series['name']}'")

        else:
            try:
                cur.execute(
                    """
                    INSERT INTO series_episodes (
                        episode_id, season_id, series_id, episode_number, episode_name,
                        overview, air_date, runtime, still_path, vote_average, vote_count, last_updated
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    """,
                    (
                        episode.get("id"),
                        season_id,
                        series_id,
                        episode_number,
                        fields["episode_name"],
                        fields["overview"],
                        fields["air_date"],
                        fields["runtime"],
                        fields["still_path"],
                        fields["vote_average"],
                        fields["vote_count"],
                    ),
                )
                print(f"📥 Inserted episode {episode_number} into season {season_id}")

                context = json.dumps({
                    "action": "insert",
                    "entity": "series_episode",
                    "episode_number": episode_number,
                    "source": "episode_sync_pipeline",
                    "timestamp": datetime.utcnow().isoformat(),
                })

                log_update(
                    cur,
                    content_id=series_id,
                    content_title=series["name"],
                    content_type="tv",
                    update_type="episode_added",
                    field_name="episode",
                    previous_value=None,
                    current_value=episode_number,
                    context=context,
                    source="backend_script",
                    timestamp=datetime.utcnow().isoformat(),
                )

                inserted += 1
                print(f"📺 Episode {episode_number} inserted for series '{series['name']}'")

            except Exception as e:
                print(f"❌ Failed to insert episode {episode_number}: {e}")

    print(f"✅ Episode sync complete: {inserted} inserted, {updated} updated")


def sync_series_seasons(cur, series_data):
    series_id = series_data["id"]
    seasons = series_data.get("seasons", [])
    print(f"🚨 Syncing seasons for series_id={series_id}, name={series_data.get('name')}")

    for season in seasons:
        season_id = season.get("id")
        season_number = season.get("season_number")
        season_name = season.get("name")
        overview = season.get("overview")
        air_date = parse_date(season.get("air_date"))
        poster_path = season.get("poster_path")

        if not season_id:
            print(f"⚠️ Skipping season with missing ID: {season}")
            continue

        # Check if season exists
        cur.execute(
            """
            SELECT season_name, overview, air_date, poster_path
            FROM series_seasons
            WHERE season_id = %s
            """,
            (season_id,)
        )
        existing = cur.fetchone()

        fields = {
            "season_name": season_name,
            "overview": overview,
            "air_date": air_date,
            "poster_path": poster_path,
        }

        if existing:
            column_names = ["season_name", "overview", "air_date", "poster_path"]
            existing_data = dict(zip(column_names, existing))

            updates = {}
            for field, new_value in fields.items():
                old_value = existing_data.get(field)
                if new_value != old_value:
                    updates[field] = (old_value, new_value)

            if updates:
                set_clause = ", ".join([f"{field} = %s" for field in updates])
                values = [new for _, new in updates.values()]
                values.append(season_id)

                cur.execute(
                    f"""
                    UPDATE series_seasons
                    SET {set_clause}, last_updated = CURRENT_TIMESTAMP
                    WHERE season_id = %s
                    """,
                    values
                )
                print(f"🔧 Season {season_number} updated: {list(updates.keys())}")

                for field, (old, new) in updates.items():
                    try:
                        context = json.dumps({
                            "action": "update",
                            "entity": "series_season",
                            "season_number": season_number,
                            "field": field,
                            "old": safe_json(old),
                            "new": safe_json(new),
                            "source": "season_sync_pipeline",
                            "timestamp": datetime.utcnow().isoformat(),
                        })

                        log_update(
                            cur,
                            content_id=series_id,
                            content_title=series_data["name"],
                            content_type="tv",
                            update_type="season_updated",
                            field_name=field,
                            previous_value=old,
                            current_value=new,
                            context=context,
                            source="backend_script",
                            timestamp=datetime.utcnow().isoformat(),
                        )
                    except Exception as e:
                        print(f"❌ Failed to log season update for field '{field}': {e}")
        else:
            # Insert new season
            cur.execute(
                """
                INSERT INTO series_seasons (
                    season_id, series_id, season_number, season_name,
                    overview, air_date, poster_path, last_updated
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """,
                (season_id, series_id, season_number, season_name, overview, air_date, poster_path)
            )
            print(f"📘 Season {season_number} inserted")

            try:
                context = json.dumps({
                    "action": "insert",
                    "entity": "series_season",
                    "season_number": season_number,
                    "source": "season_sync_pipeline",
                    "timestamp": datetime.utcnow().isoformat(),
                })

                log_update(
                    cur,
                    content_id=series_id,
                    content_title=series_data["name"],
                    content_type="tv",
                    update_type="season_added",
                    field_name="season",
                    previous_value=None,
                    current_value=season_number,
                    context=context,
                    source="backend_script",
                    timestamp=datetime.utcnow().isoformat(),
                )
            except Exception as e:
                print(f"❌ Failed to log season insert for season {season_number}: {e}")
