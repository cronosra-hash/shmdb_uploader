from datetime import datetime
from db.logger import log_update
from psycopg2 import sql
import traceback
import json


def update_movie_data(conn, movie, media_type, verbose=False):
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
        fields_dict = extract_movie_fields(movie)

        # Compare using schema-aware logic
        updates, values, changed_fields = compare_fields(
            existing_row, column_names, fields_dict, verbose=verbose
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

            context = json.dumps(
                {
                    "action": "update",
                    "field": field,
                    "previous": old,
                    "current": new,
                    "source": "movie_update_pipeline",
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

            log_update(
                cur,
                content_id=movie["movie_id"],
                content_title=movie.get("movie_title") or movie.get("title"),
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
            print(f"✅ No changes for movie: {movie['title']}")

    return fields_dict


def compare_fields(existing_row, column_names, fields, verbose=False):
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
                pass  # keep as string if coercion fails

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
            print(f"🔄 Field changed: {field}: '{old}' ➡️ '{new}'")

    return updates, values, changed_fields


def insert_movie_data(conn, movie, media_type, verbose=False):
    with conn.cursor() as cur:
        fields_dict = extract_movie_fields(movie)
        fields_dict["last_updated"] = sql.SQL("NOW()")  # raw SQL expression

        field_identifiers = []
        value_placeholders = []
        param_values = []

        for field, value in fields_dict.items():
            field_identifiers.append(sql.Identifier(field))
            if isinstance(value, sql.SQL):
                value_placeholders.append(value)
            else:
                value_placeholders.append(sql.Placeholder())
                param_values.append(value)

        query = sql.SQL("""
            INSERT INTO movies ({fields})
            VALUES ({values})
            ON CONFLICT (movie_id) DO NOTHING;
        """).format(
            fields=sql.SQL(", ").join(field_identifiers),
            values=sql.SQL(", ").join(value_placeholders),
        )

        cur.execute(query, param_values)
        conn.commit()

        movie_id = movie.get("movie_id") or movie.get("id")
        title = movie.get("title", "[unknown title]")

        print(f"🆕 Inserting movie_id={movie_id} ({title})")
        print(f"✅ Inserted fields: {list(fields_dict.keys())}")

        null_fields = [k for k, v in fields_dict.items() if v is None]
        if null_fields:
            print(f"⚠️ Null fields: {null_fields}")

        print(f"🆕 Inserted movie: {movie['title']}")

        for field, value in fields_dict.items():
            if not isinstance(value, sql.SQL):  # skip raw SQL expressions
                context = json.dumps(
                    {
                        "action": "insert",
                        "field": field,
                        "value": value,
                        "source": "movie_insert_pipeline",
                        "timestamp": datetime.utcnow().isoformat(),
                    }
                )

                log_update(
                    cur,
                    content_id=movie_id,
                    content_title=title,
                    content_type=media_type,
                    update_type="field_inserted",
                    field_name=field,
                    previous_value=None,
                    current_value=value,
                    context=context,
                    source="backend_script",
                    timestamp=datetime.utcnow(),
                )

    return fields_dict


def insert_collection_if_needed(cur, collection):
    if not collection:
        return

    cur.execute(
        """
        INSERT INTO collections (id, collection_name, overview, poster_path, backdrop_path)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
    """,
        (
            collection["id"],
            collection["collection_name"],
            collection.get("overview"),
            collection.get("poster_path"),
            collection.get("backdrop_path"),
        ),
    )


def extract_movie_fields(data):
    """
    Extracts relevant fields from a movie dict for SQL insertion.
    Returns a flat dict of field → value.
    """
    return {
        "movie_id": data.get("id"),
        "movie_title": data.get("title"),
        "overview": data.get("overview"),
        "popularity": data.get("popularity"),
        "vote_average": data.get("vote_average"),
        "vote_count": data.get("vote_count"),
        "poster_path": data.get("poster_path"),
        "backdrop_path": data.get("backdrop_path"),
        "original_language": data.get("original_language"),
        "release_date": data.get("release_date"),
        "runtime": data.get("runtime"),
        "status": data.get("status"),
        "budget": data.get("budget"),
        "revenue": data.get("revenue"),
    }


def insert_or_update_movie_data(conn, movie, media_type):
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
            update_movie_data(conn, movie, media_type)

        else:
            print(f"🆕 Inserting movie_id={movie_id}")
            insert_movie_data(conn, movie, media_type)

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

            context = json.dumps(
                {
                    "action": "link",
                    "entity": "genre",
                    "genre_name": genre["name"],
                    "source": "genre_link_pipeline",
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

            log_update(
                cur,
                content_id=movie_id,
                content_title=movie["movie_title"],
                content_type="movie",
                update_type="genre_added",
                field_name="genre",
                previous_value=None,
                current_value=genre["name"],
                context=context,
                source="backend_script",
                timestamp=datetime.utcnow(),
            )

            print(f"Genre '{genre['name']}' linked to movie '{movie['movie_title']}'")


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

            context = json.dumps(
                {
                    "action": "link",
                    "entity": "company",
                    "company_name": company["name"],
                    "source": "company_link_pipeline",
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

            log_update(
                cur,
                content_id=movie_id,
                content_title=movie["movie_title"],
                content_type="movie",
                update_type="company_added",
                field_name="company",
                previous_value=None,
                current_value=company["name"],
                context=context,
                source="backend_script",
                timestamp=datetime.utcnow(),
            )

            print(
                f"Company '{company['name']}' linked to movie '{movie['movie_title']}'"
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
            context = json.dumps(
                {
                    "action": "link",
                    "entity": "language",
                    "language_name": lang["name"],
                    "source": "language_link_pipeline",
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

            log_update(
                cur,
                content_id=movie_id,
                content_title=movie["movie_title"],
                content_type="movie",
                update_type="language_added",
                field_name="language",
                previous_value=None,
                current_value=lang["name"],
                context=context,
                source="backend_script",
                timestamp=datetime.utcnow(),
            )

            print(f"Language '{lang['name']}' linked to movie '{movie['movie_title']}'")


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

            context = json.dumps(
                {
                    "action": "link",
                    "entity": "country",
                    "country_name": country["name"],
                    "source": "country_link_pipeline",
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

            log_update(
                cur,
                content_id=movie_id,
                content_title=movie["movie_title"],
                content_type="movie",
                update_type="country_added",
                field_name="country",
                previous_value=None,
                current_value=country["name"],
                context=context,
                source="backend_script",
                timestamp=datetime.utcnow(),
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
                content_id=movie_id,
                content_title=movie["movie_title"],
                content_type="movie",
                update_type="cast_added",
                field_name="cast",
                previous_value=None,
                current_value=cast["name"],
                context=context,
                source="backend_script",
                timestamp=datetime.utcnow(),
            )

            print(f"Cast '{cast['name']}' linked to movie '{movie['movie_title']}'")


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

            context = json.dumps(
                {
                    "action": "link",
                    "entity": "crew",
                    "crew_name": crew["name"],
                    "source": "crew_link_pipeline",
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

            log_update(
                cur,
                content_id=movie_id,
                content_title=movie["movie_title"],
                content_type="movie",
                update_type="crew_added",
                field_name="crew",
                previous_value=None,
                current_value=crew["name"],
                context=context,
                source="backend_script",
                timestamp=datetime.utcnow(),
            )

            print(
                f"🎬 Crew '{crew['name']}' ({crew.get('job')}) linked to movie '{movie['movie_title']}'"
            )
