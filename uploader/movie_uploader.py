from datetime import datetime
from db.logger import log_update
from tmdb.movie_api import get_movie_data

def update_movie_data(conn, movie):
    """
    Updates an existing movie record in the database if any fields have changed.
    Also logs each change for auditing purposes.

    Args:
        conn: A database connection object.
        movie (dict): A dictionary containing updated movie data from TMDB.
    """

    with conn.cursor() as cur:
        # Fetch the existing movie record from the database
        cur.execute("SELECT * FROM movies WHERE id = %s;", (movie["id"],))
        existing = cur.fetchone()

        # List of fields to compare and potentially update
        fields = [
            "title", "original_title", "overview", "release_date", "runtime",
            "popularity", "vote_average", "vote_count", "poster_path", "backdrop_path",
            "original_language", "status", "budget", "revenue", "homepage", "imdb_id", "collection_id"
        ]

        # Prepare containers for SQL update statements and values
        updates, values, changed_fields = [], [], []

        # Get collection ID if the movie belongs to a collection
        collection = movie.get("belongs_to_collection")
        collection_id = collection["id"] if collection else None

        # Extract new data values from the movie dictionary
        new_data = [
            movie["title"], movie["original_title"], movie["overview"], movie.get("release_date"),
            movie.get("runtime"), movie.get("popularity"), movie.get("vote_average"), movie.get("vote_count"),
            movie.get("poster_path"), movie.get("backdrop_path"), movie.get("original_language"),
            movie.get("status"), movie.get("budget"), movie.get("revenue"), movie.get("homepage"),
            movie.get("imdb_id"), collection_id
        ]

        # Compare each field with the existing value
        for i, (field, new_value) in enumerate(zip(fields, new_data)):
            old_value = existing[i + 1]  # Offset by 1 because first column is usually the ID

            # Convert release_date string to date object if necessary
            if field == "release_date" and isinstance(new_value, str):
                try:
                    new_value = datetime.strptime(new_value.strip(), "%Y-%m-%d").date()
                except ValueError:
                    pass  # Ignore invalid date formats

            # If the value has changed, prepare it for update
            if new_value != old_value:
                updates.append(f"{field} = %s")
                values.append(new_value)
                changed_fields.append((field, old_value, new_value))

                # Log the change for auditing
                log_update(cur, movie["id"], movie["title"], "field_change", field, old_value, new_value)

        # If there are any changes, update the record
        if updates:
            updates.append("lastupdated = NOW()")  # Update the timestamp
            values.append(movie["id"])  # Add movie ID for WHERE clause
            cur.execute(f"UPDATE movies SET {', '.join(updates)} WHERE id = %s;", values)
            conn.commit()

            # Print summary of changes
            print(f"🔄 Updated movie: {movie['title']}")
            for field, old, new in changed_fields:
                print(f" - {field}: '{old}' ➡️ '{new}'")
        else:
            # No changes detected
            print(f"✅ No changes for movie: {movie['title']}")

def insert_movie_data(conn, movie):
    """
    Inserts a new movie record into the database, including its collection (if any).

    Args:
        conn: A database connection object.
        movie (dict): A dictionary containing movie data from TMDB.
    """

    with conn.cursor() as cur:
        # Check if the movie belongs to a collection (e.g., a franchise like "Harry Potter Collection")
        collection = movie.get("belongs_to_collection")
        if collection:
            # Insert the collection into the 'collections' table if it doesn't already exist
            cur.execute("""
                INSERT INTO collections (id, name, overview, poster_path, backdrop_path)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                collection["id"], collection["name"], collection.get("overview"),
                collection.get("poster_path"), collection.get("backdrop_path")
            ))

        # Insert the movie into the 'movies' table
        cur.execute("""
            INSERT INTO movies (
                id, title, original_title, overview, release_date, runtime,
                popularity, vote_average, vote_count, poster_path, backdrop_path,
                original_language, status, budget, revenue, homepage, imdb_id,
                collection_id, lastupdated
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (id) DO NOTHING;
        """, (
            movie["id"], movie["title"], movie["original_title"], movie["overview"],
            movie.get("release_date"), movie.get("runtime"), movie.get("popularity"),
            movie.get("vote_average"), movie.get("vote_count"), movie.get("poster_path"),
            movie.get("backdrop_path"), movie.get("original_language"), movie.get("status"),
            movie.get("budget"), movie.get("revenue"), movie.get("homepage"),
            movie.get("imdb_id"), collection["id"] if collection else None
        ))

        # Commit the transaction to save changes
        conn.commit()

        # Log success message to console
        print(f"✅ Inserted movie: {movie['title']}")

def insert_or_update_movie_data(conn, movie):
    """
    Inserts or updates movie data in the database, including genres, production companies,
    spoken languages, production countries, cast, and crew.

    Args:
        conn: A database connection object.
        movie (dict): A dictionary containing movie data from TMDB.
    """

    # Check if the movie already exists in the database
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM movies WHERE id = %s;", (movie["id"],))
        existing = cur.fetchone()

    # Update or insert the movie record based on existence
    if existing:
        update_movie_data(conn, movie)
    else:
        insert_movie_data(conn, movie)

    movie_id = movie["id"]

    # Insert genres and link them to the movie
    with conn.cursor() as cur:
        for genre in movie.get("genres", []):
            # Insert genre if it doesn't exist
            cur.execute("INSERT INTO genres (id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING;", (genre["id"], genre["name"]))
            # Link genre to movie if not already linked
            cur.execute("SELECT 1 FROM movie_genres WHERE movie_id = %s AND genre_id = %s;", (movie_id, genre["id"]))
            if not cur.fetchone():
                cur.execute("INSERT INTO movie_genres (movie_id, genre_id) VALUES (%s, %s);", (movie_id, genre["id"]))
                log_update(cur, movie_id, movie["title"], "genre_added", "genre", None, genre["name"])
                print(f"➕ Added genre '{genre['name']}' to movie '{movie['title']}'")

        # Insert production companies and link them to the movie
        for company in movie.get("production_companies", []):
            cur.execute("""
                INSERT INTO production_companies (id, name, logo_path, origin_country)
                VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;
            """, (
                company["id"], company["name"], company.get("logo_path"), company.get("origin_country")
            ))
            cur.execute("SELECT 1 FROM movie_companies WHERE movie_id = %s AND company_id = %s;", (movie_id, company["id"]))
            if not cur.fetchone():
                cur.execute("INSERT INTO movie_companies (movie_id, company_id) VALUES (%s, %s);", (movie_id, company["id"]))
                log_update(cur, movie_id, movie["title"], "company_added", "company", None, company["name"])
                print(f"🏢 Added production company '{company['name']}' to movie '{movie['title']}'")

        # Insert spoken languages and link them to the movie
        for lang in movie.get("spoken_languages", []):
            cur.execute("INSERT INTO spoken_languages (iso_639_1, name) VALUES (%s, %s) ON CONFLICT DO NOTHING;", (lang["iso_639_1"], lang["name"]))
            cur.execute("SELECT 1 FROM movie_languages WHERE movie_id = %s AND language_code = %s;", (movie_id, lang["iso_639_1"]))
            if not cur.fetchone():
                cur.execute("INSERT INTO movie_languages (movie_id, language_code) VALUES (%s, %s);", (movie_id, lang["iso_639_1"]))
                log_update(cur, movie_id, movie["title"], "language_added", "language", None, lang["name"])
                print(f"🗣️ Added spoken language '{lang['name']}' to movie '{movie['title']}'")

        # Insert production countries and link them to the movie
        for country in movie.get("production_countries", []):
            cur.execute("INSERT INTO countries (iso_3166_1, name) VALUES (%s, %s) ON CONFLICT DO NOTHING;", (country["iso_3166_1"], country["name"]))
            cur.execute("SELECT 1 FROM movie_countries WHERE movie_id = %s AND country_code = %s;", (movie_id, country["iso_3166_1"]))
            if not cur.fetchone():
                cur.execute("INSERT INTO movie_countries (movie_id, country_code) VALUES (%s, %s);", (movie_id, country["iso_3166_1"]))
                log_update(cur, movie_id, movie["title"], "country_added", "country", None, country["name"])
                print(f"🌍 Added production country '{country['name']}' to movie '{movie['title']}'")

        # Insert top 10 cast members and link them to the movie
        credits = movie.get("credits", {})
        for cast in credits.get("cast", [])[:10]:
            cur.execute("""
                INSERT INTO people (person_id, name, gender, profile_path, known_for_department, popularity)
                VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;
            """, (
                cast["id"], cast["name"], cast["gender"], cast.get("profile_path"),
                cast.get("known_for_department"), cast.get("popularity")
            ))
            cur.execute("SELECT 1 FROM moviecast WHERE movie_id = %s AND person_id = %s;", (movie_id, cast["id"]))
            if not cur.fetchone():
                cur.execute("""
                    INSERT INTO moviecast (movie_id, person_id, character_name, cast_order)
                    VALUES (%s, %s, %s, %s);
                """, (movie_id, cast["id"], cast.get("character"), cast.get("order")))
                log_update(cur, movie_id, movie["title"], "cast_added", "cast", None, cast["name"])
                print(f"🎭 Added cast member '{cast['name']}' as '{cast.get('character')}' to movie '{movie['title']}'")

        # Insert crew members and link them to the movie
        for crew in credits.get("crew", []):
            cur.execute("""
                INSERT INTO people (person_id, name, gender, profile_path, known_for_department, popularity)
                VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;
            """, (
                crew["id"], crew["name"], crew["gender"], crew.get("profile_path"),
                crew.get("known_for_department"), crew.get("popularity")
            ))
            cur.execute("SELECT 1 FROM moviecrew WHERE movie_id = %s AND person_id = %s;", (movie_id, crew["id"]))
            if not cur.fetchone():
                cur.execute("""
                    INSERT INTO moviecrew (movie_id, person_id, department, job)
                    VALUES (%s, %s, %s, %s);
                """, (movie_id, crew["id"], crew.get("department"), crew.get("job")))
                log_update(cur, movie_id, movie["title"], "crew_added", "crew", None, crew["name"])
                print(f"🎬 Added crew member '{crew['name']}' ({crew.get('job')}) to movie '{movie['title']}'")

        # Commit all changes to the database
        conn.commit()
