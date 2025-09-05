from datetime import datetime
from db.logger import log_update
from tmdb.tv_api import fetch_series, fetch_season
from utils import parse_date
import requests
import traceback
from psycopg2 import sql


def update_series_data(conn, series, media_type="series", verbose=False):
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
                log_update(
                    cur,
                    series_id,
                    series_title,
                    media_type,
                    "field_updated",
                    field,
                    old,
                    new,
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
        "name": series.get("title"),
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

                print(f"✅ Inserted Series: {series_title}")
                for field, old, new in changed_fields:
                    print(f" - {field}: '{old}' ➡️ '{new}'")
                    log_update(
                        cur,
                        series_id,
                        series_title,
                        media_type,
                        "field_updated",
                        field,
                        old,
                        new,
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
                    log_update(
                        cur,
                        series_id,
                        series_title,
                        media_type,
                        "field_inserted",
                        field,
                        None,
                        value,
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
    cast_list = series.get("credits", {}).get("cast", [])[:30]
    series_title = series.get("name")

    for cast in cast_list:
        ensure_person_exists(cur, cast)
        cur.execute(
            """
            SELECT 1 FROM series_cast
            WHERE series_id = %s AND person_id = %s;
        """,
            (series_id, cast["id"]),
        )

        if not cur.fetchone():
            print(
                f"🎭 Added cast member '{cast['name']}' as '{cast.get('character')}' to series '{series_title}'"
            )
            log_update(
                cur, series_id, series_title, "cast_added", "cast", None, cast["name"]
            )
            cur.execute(
                """
                INSERT INTO series_cast (
                    series_id, person_id, cast_order, character_name, last_updated
                )
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT DO NOTHING;
            """,
                (series_id, cast["id"], cast.get("order", 0), cast.get("character")),
            )


def insert_series_crew(cur, series_id, series):
    crew_list = series.get("credits", {}).get("crew", [])
    series_title = series.get("name")

    for crew in crew_list:
        ensure_person_exists(cur, crew)
        cur.execute(
            """
            SELECT 1 FROM series_crew
            WHERE series_id = %s AND person_id = %s AND job = %s;
        """,
            (series_id, crew["id"], crew.get("job")),
        )

        if not cur.fetchone():
            print(
                f"🎬 Added crew member '{crew['name']}' as '{crew.get('job')}' to series '{series_title}'"
            )
            log_update(
                cur, series_id, series_title, "crew_added", "crew", None, crew["name"]
            )
            cur.execute(
                """
                INSERT INTO series_crew (
                    series_id, person_id, department, job, last_updated
                )
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT DO NOTHING;
            """,
                (series_id, crew["id"], crew.get("department"), crew.get("job")),
            )


def insert_series_genres(cur, series_id, series):
    genres = series.get("genres", [])
    series_title = series.get("name")

    for genre in genres:
        genre_id = genre.get("id")
        genre_name = genre.get("name")

        if not genre_id or not genre_name:
            continue

        cur.execute(
            """
            INSERT INTO genres (genre_id, genre_name)
            VALUES (%s, %s)
            ON CONFLICT (genre_id) DO NOTHING;
        """,
            (genre_id, genre_name),
        )

        cur.execute(
            """
            SELECT 1 FROM series_genres
            WHERE series_id = %s AND genre_id = %s;
        """,
            (series_id, genre_id),
        )

        if not cur.fetchone():
            print(f"🎨 Linked genre '{genre_name}' to series '{series_title}'")
            log_update(
                cur,
                series_id,
                series_title,
                "genre_added",
                "genre_name",
                None,
                genre_name,
            )
            cur.execute(
                """
                INSERT INTO series_genres (
                    series_id, genre_id, last_updated
                )
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT DO NOTHING;
            """,
                (series_id, genre_id),
            )

def insert_series_companies(cur, series_id, series):
    series_title = series.get("name")

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
            log_update(
                cur,
                series_id,
                series_title,
                "company_added",
                "company",
                None,
                company["name"],
            )
            print(
                f"🎨 Linked company '{company['name']}' to series '{series_title}'"
            )

def insert_series_languages(cur, series_id, series):
    series_title = series.get("name")

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
            log_update(
                cur,
                series_id,
                series_title,
                "language_added",
                "language",
                None,
                lang["name"],
            )
            print(
                f"🎨 Linked language '{lang['name']}' to series '{series_title}'"
            )

def insert_series_countries(cur, series_id, series):
    series_title = series.get("name")

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
            log_update(
                cur,
                series_id,
                series_title,
                "country_added",
                "country",
                None,
                country_code,
            )
            print(
                f"🎨 Linked country '{country_code}' to series '{series_title}'"
            )
