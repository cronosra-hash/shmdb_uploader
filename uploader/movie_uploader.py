import traceback
from datetime import datetime
from db.logger import log_update
from tmdb.movie_api import get_movie_data
from psycopg2 import sql


def update_movie_data(conn, movie, verbose=False):
    """
    Updates an existing movie record in the database if any fields have changed.
    Uses schema-aware comparison and logs each change for auditing.
    """
    with conn.cursor() as cur:
        # Fetch existing row and column names
        cur.execute("SELECT * FROM movies WHERE movie_id = %s;", (movie["movie_id"],))
        existing_row = cur.fetchone()

        if not existing_row:
            print(f"⚠️ Movie ID {movie['movie_id']} not found in database.")
            return

        column_names = [desc[0] for desc in cur.description]
        fields, new_data = extract_movie_fields(movie)

        # Compare using schema-aware logic
        updates, values, changed_fields = compare_fields(
            existing_row, column_names, fields, new_data, verbose=verbose
        )

        if updates:
            updates.append("last_updated = NOW()")
            values.append(movie["movie_id"])

            query = sql.SQL("UPDATE movies SET {} WHERE movie_id = %s;").format(
                sql.SQL(", ").join(map(sql.SQL, updates))
            )
            cur.execute(query, values)
            conn.commit()

            print(f"🔄 Updated movie: {movie['title']}")
            for field, old, new in changed_fields:
                print(f" - {field}: '{old}' ➡️ '{new}'")
                log_update(
                    cur,
                    movie["movie_id"],
                    movie.get("movie_title") or movie.get("title"),
                    "field_updated",
                    field,
                    old,
                    new,
                )
            else:
                print(f"✅ No changes for movie: {movie['title']}")

    return fields, new_data


def compare_fields(existing_row, column_names, fields, new_data, verbose=False):
    updates, values, changed_fields = [], [], []

    # Map column names to their current values
    existing_dict = dict(zip(column_names, existing_row))

    for field, new_value in zip(fields, new_data):
        old_value = existing_dict.get(field)

        # Normalize release_date
        if field == "release_date" and isinstance(new_value, str):
            try:
                new_value = datetime.strptime(new_value.strip(), "%Y-%m-%d").date()
            except ValueError:
                if verbose:
                    print(f"⚠️ Skipping invalid release_date: {new_value}")
                continue

        # Normalize strings
        if isinstance(old_value, str) and isinstance(new_value, str):
            old_value = old_value.strip()
            new_value = new_value.strip()

        # Attempt type coercion for comparison
        if isinstance(old_value, (int, float)) and isinstance(new_value, str):
            try:
                new_value = type(old_value)(new_value)
            except ValueError:
                pass

        # Detect change
        if new_value != old_value:
            updates.append(f"{field} = %s")
            values.append(new_value)
            changed_fields.append((field, old_value, new_value))

            if verbose:
                print(f"🔄 {field}: '{old_value}' → '{new_value}'")

    return updates, values, changed_fields


def insert_movie_data(conn, movie, verbose=False):
    """
    Inserts a new movie record into the database, including its collection (if any).
    Uses safe SQL construction and logs insertion for auditing.
    """
    with conn.cursor() as cur:
        # Ensure collection exists
        insert_collection_if_needed(cur, movie.get("belongs_to_collection"))

        # Extract fields and values
        fields, values = extract_movie_fields(movie)
        fields.append("last_updated")
        values.append("NOW()")  # We'll handle this as a raw SQL expression below

        # Build SQL safely
        field_identifiers = [sql.Identifier(f) for f in fields]
        value_placeholders = [
            sql.Placeholder() if v != "NOW()" else sql.SQL("NOW()") for v in values
        ]

        query = sql.SQL("""
            INSERT INTO movies ({fields})
            VALUES ({values})
            ON CONFLICT (movie_id) DO NOTHING;
        """).format(
            fields=sql.SQL(", ").join(field_identifiers),
            values=sql.SQL(", ").join(value_placeholders),
        )

        # Filter out raw SQL expressions from values
        param_values = [v for v in values if v != "NOW()"]
        cur.execute(query, param_values)

    conn.commit()
    print(f"✅ Inserted movie: {movie['title']}")
    if verbose:
        print(f"Fields inserted: {fields}")


def insert_collection_if_needed(cur, collection):
    if not collection:
        return

    cur.execute(
        """
        INSERT INTO collections (collection_id, collection_name, overview, poster_path, backdrop_path)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (collection_id) DO NOTHING;
    """,
        (
            collection["id"],
            collection["name"],
            collection.get("overview"),
            collection.get("poster_path"),
            collection.get("backdrop_path"),
        ),
    )


def extract_movie_fields(movie):
    collection = movie.get("belongs_to_collection")
    collection_id = collection["id"] if collection else None

    field_map = {
        "movie_id": movie.get("movie_id") or movie.get("id"),
        "movie_title": movie.get("movie_title") or movie.get("title"),
        "original_title": movie.get("original_title"),
        "overview": movie.get("overview"),
        "release_date": movie.get("release_date"),
        "runtime": movie.get("runtime"),
        "popularity": movie.get("popularity"),
        "vote_average": movie.get("vote_average"),
        "vote_count": movie.get("vote_count"),
        "poster_path": movie.get("poster_path"),
        "backdrop_path": movie.get("backdrop_path"),
        "original_language": movie.get("original_language"),
        "status": movie.get("status"),
        "budget": movie.get("budget"),
        "revenue": movie.get("revenue"),
        "homepage": movie.get("homepage"),
        "imdb_id": movie.get("imdb_id"),
        "collection_id": collection_id,
    }

    fields = list(field_map.keys())
    values = list(field_map.values())

    return fields, values


def insert_or_update_movie_data(conn, movie):
    """
    Inserts or updates movie data in the database, including genres, production companies,
    spoken languages, production countries, cast, and crew.
    """
    movie = normalize_movie_payload(movie)
    movie_id = movie.get("movie_id") or movie.get("id")
    movie["movie_id"] = movie_id

    if not movie_id:
        print("❌ No movie ID found in payload — skipping.")
        return

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM movies WHERE movie_id = %s;", (movie_id,))
            exists = cur.fetchone()

        if exists:
            print(f"🔄 Updating movie_id={movie_id}")
            update_movie_data(conn, movie)
        else:
            print(f"🆕 Inserting movie_id={movie_id}")
            insert_movie_data(conn, movie)

        with conn.cursor() as cur:
            insert_genres(cur, movie_id, movie)
            insert_production_companies(cur, movie_id, movie)
            insert_spoken_languages(cur, movie_id, movie)
            insert_production_countries(cur, movie_id, movie)
            insert_cast(cur, movie_id, movie)
            insert_crew(cur, movie_id, movie)

        conn.commit()
        print(f"✅ Movie ID {movie_id} processed successfully.")

    except Exception as e:
        print(f"❌ Failed to insert/update movie_id={movie_id}: {e}")
        traceback.print_exc()
        conn.rollback()


def normalize_movie_payload(movie):
    movie["movie_id"] = movie.get("movie_id") or movie.get("id")
    movie["movie_title"] = (
        movie.get("movie_title") or movie.get("title") or f"ID {movie['movie_id']}"
    )
    return movie


def insert_genres(cur, movie_id, movie):
    for genre in movie.get("genres", []):
        cur.execute(
            """
            INSERT INTO genres (genre_id, genre_name)
            VALUES (%s, %s) ON CONFLICT DO NOTHING;
        """,
            (genre["id"], genre["name"]),
        )

        cur.execute(
            """
            SELECT 1 FROM movie_genres WHERE movie_id = %s AND genre_id = %s;
        """,
            (movie_id, genre["id"]),
        )
        if not cur.fetchone():
            cur.execute(
                """
                INSERT INTO movie_genres (movie_id, genre_id) VALUES (%s, %s);
            """,
                (movie_id, genre["id"]),
            )
            log_update(
                cur,
                movie_id,
                movie["movie_title"],
                "genre_added",
                "genre",
                None,
                genre["name"],
            )
            print(
                f"➕ Genre '{genre['name']}' linked to movie '{movie['movie_title']}'"
            )


def insert_production_companies(cur, movie_id, movie):
    for company in movie.get("production_companies", []):
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
            SELECT 1 FROM movie_companies WHERE movie_id = %s AND company_id = %s;
        """,
            (movie_id, company["id"]),
        )
        if not cur.fetchone():
            cur.execute(
                """
                INSERT INTO movie_companies (movie_id, company_id) VALUES (%s, %s);
            """,
                (movie_id, company["id"]),
            )
            log_update(
                cur,
                movie_id,
                movie["movie_title"],
                "company_added",
                "company",
                None,
                company["name"],
            )
            print(
                f"🏢 Company '{company['name']}' linked to movie '{movie['movie_title']}'"
            )


def insert_spoken_languages(cur, movie_id, movie):
    for lang in movie.get("spoken_languages", []):
        cur.execute(
            """
            INSERT INTO spoken_languages (iso_639_1, language_name)
            VALUES (%s, %s) ON CONFLICT DO NOTHING;
        """,
            (lang["iso_639_1"], lang["name"]),
        )

        cur.execute(
            """
            SELECT 1 FROM movie_languages WHERE movie_id = %s AND language_code = %s;
        """,
            (movie_id, lang["iso_639_1"]),
        )
        if not cur.fetchone():
            cur.execute(
                """
                INSERT INTO movie_languages (movie_id, language_code) VALUES (%s, %s);
            """,
                (movie_id, lang["iso_639_1"]),
            )
            log_update(
                cur,
                movie_id,
                movie["movie_title"],
                "language_added",
                "language",
                None,
                lang["name"],
            )
            print(
                f"🗣️ Language '{lang['name']}' linked to movie '{movie['movie_title']}'"
            )


def insert_production_countries(cur, movie_id, movie):
    for country in movie.get("production_countries", []):
        cur.execute(
            """
            INSERT INTO countries (iso_3166_1, country_name)
            VALUES (%s, %s) ON CONFLICT DO NOTHING;
        """,
            (country["iso_3166_1"], country["name"]),
        )

        cur.execute(
            """
            SELECT 1 FROM movie_countries WHERE movie_id = %s AND country_code = %s;
        """,
            (movie_id, country["iso_3166_1"]),
        )
        if not cur.fetchone():
            cur.execute(
                """
                INSERT INTO movie_countries (movie_id, country_code) VALUES (%s, %s);
            """,
                (movie_id, country["iso_3166_1"]),
            )
            log_update(
                cur,
                movie_id,
                movie["movie_title"],
                "country_added",
                "country",
                None,
                country["name"],
            )
            print(
                f"🌍 Country '{country['name']}' linked to movie '{movie['movie_title']}'"
            )


def insert_cast(cur, movie_id, movie):
    for cast in movie.get("credits", {}).get("cast", [])[:10]:
        cur.execute(
            """
            INSERT INTO people (person_id, name, gender, profile_path, known_for_department, popularity)
            VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;
        """,
            (
                cast["id"],
                cast["name"],
                cast["gender"],
                cast.get("profile_path"),
                cast.get("known_for_department"),
                cast.get("popularity"),
            ),
        )

        cur.execute(
            """
            SELECT 1 FROM movie_cast WHERE movie_id = %s AND actor_id = %s;
        """,
            (movie_id, cast["id"]),
        )
        if not cur.fetchone():
            cur.execute(
                """
                INSERT INTO movie_cast (movie_id, actor_id, character_name, cast_order)
                VALUES (%s, %s, %s, %s);
            """,
                (movie_id, cast["id"], cast.get("character"), cast.get("order")),
            )
            log_update(
                cur,
                movie_id,
                movie["movie_title"],
                "cast_added",
                "cast",
                None,
                cast["name"],
            )
            print(
                f"🎭 Cast '{cast['name']}' as '{cast.get('character')}' added to movie '{movie['movie_title']}'"
            )


def insert_crew(cur, movie_id, movie):
    for crew in movie.get("credits", {}).get("crew", []):
        cur.execute(
            """
            INSERT INTO people (person_id, name, gender, profile_path, known_for_department, popularity)
            VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;
        """,
            (
                crew["id"],
                crew["name"],
                crew["gender"],
                crew.get("profile_path"),
                crew.get("known_for_department"),
                crew.get("popularity"),
            ),
        )

        cur.execute(
            """
            SELECT 1 FROM movie_crew WHERE movie_id = %s AND crew_member_id = %s;
        """,
            (movie_id, crew["id"]),
        )
        if not cur.fetchone():
            cur.execute(
                """
                INSERT INTO movie_crew (movie_id, crew_member_id, department, job)
                VALUES (%s, %s, %s, %s);
            """,
                (movie_id, crew["id"], crew.get("department"), crew.get("job")),
            )
            log_update(
                cur,
                movie_id,
                movie["movie_title"],
                "crew_added",
                "crew",
                None,
                crew["name"],
            )
            print(
                f"🎬 Crew '{crew['name']}' ({crew.get('job')}) added to movie '{movie['movie_title']}'"
            )
