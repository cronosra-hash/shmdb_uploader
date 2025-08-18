from datetime import datetime
from db.logger import log_update
from tmdb.movie_api import get_movie_data

def update_movie_data(conn, movie):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM movies WHERE id = %s;", (movie["id"],))
        existing = cur.fetchone()

        fields = [
            "title", "original_title", "overview", "release_date", "runtime",
            "popularity", "vote_average", "vote_count", "poster_path", "backdrop_path",
            "original_language", "status", "budget", "revenue", "homepage", "imdb_id", "collection_id"
        ]

        updates, values, changed_fields = [], [], []
        collection = movie.get("belongs_to_collection")
        collection_id = collection["id"] if collection else None

        new_data = [
            movie["title"], movie["original_title"], movie["overview"], movie.get("release_date"),
            movie.get("runtime"), movie.get("popularity"), movie.get("vote_average"), movie.get("vote_count"),
            movie.get("poster_path"), movie.get("backdrop_path"), movie.get("original_language"),
            movie.get("status"), movie.get("budget"), movie.get("revenue"), movie.get("homepage"),
            movie.get("imdb_id"), collection_id
        ]

        for i, (field, new_value) in enumerate(zip(fields, new_data)):
            old_value = existing[i + 1]
            if field == "release_date" and isinstance(new_value, str):
                try:
                    new_value = datetime.strptime(new_value.strip(), "%Y-%m-%d").date()
                except ValueError:
                    pass
            if new_value != old_value:
                updates.append(f"{field} = %s")
                values.append(new_value)
                changed_fields.append((field, old_value, new_value))
                log_update(cur, movie["id"], movie["title"], "field_change", field, old_value, new_value)

        if updates:
            updates.append("lastupdated = NOW()")
            values.append(movie["id"])
            cur.execute(f"UPDATE movies SET {', '.join(updates)} WHERE id = %s;", values)
            conn.commit()
            print(f"🔄 Updated movie: {movie['title']}")
            for field, old, new in changed_fields:
                print(f" - {field}: '{old}' ➡️ '{new}'")
        else:
            print(f"✅ No changes for movie: {movie['title']}")

def insert_movie_data(conn, movie):
    with conn.cursor() as cur:
        collection = movie.get("belongs_to_collection")
        if collection:
            cur.execute("""
                INSERT INTO collections (id, name, overview, poster_path, backdrop_path)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                collection["id"], collection["name"], collection.get("overview"),
                collection.get("poster_path"), collection.get("backdrop_path")
            ))

        cur.execute("""
            INSERT INTO movies (id, title, original_title, overview, release_date, runtime,
            popularity, vote_average, vote_count, poster_path, backdrop_path,
            original_language, status, budget, revenue, homepage, imdb_id, collection_id, lastupdated)
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
        conn.commit()
        print(f"✅ Inserted movie: {movie['title']}")

def insert_or_update_movie_data(conn, movie):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM movies WHERE id = %s;", (movie["id"],))
        existing = cur.fetchone()

    if existing:
        update_movie_data(conn, movie)
    else:
        insert_movie_data(conn, movie)

    movie_id = movie["id"]
    with conn.cursor() as cur:
        for genre in movie.get("genres", []):
            cur.execute("INSERT INTO genres (id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING;", (genre["id"], genre["name"]))
            cur.execute("SELECT 1 FROM movie_genres WHERE movie_id = %s AND genre_id = %s;", (movie_id, genre["id"]))
            if not cur.fetchone():
                cur.execute("INSERT INTO movie_genres (movie_id, genre_id) VALUES (%s, %s);", (movie_id, genre["id"]))
                log_update(cur, movie_id, movie["title"], "genre_added", "genre", None, genre["name"])
                print(f"➕ Added genre '{genre['name']}' to movie '{movie['title']}'")

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

        for lang in movie.get("spoken_languages", []):
            cur.execute("INSERT INTO spoken_languages (iso_639_1, name) VALUES (%s, %s) ON CONFLICT DO NOTHING;", (lang["iso_639_1"], lang["name"]))
            cur.execute("SELECT 1 FROM movie_languages WHERE movie_id = %s AND language_code = %s;", (movie_id, lang["iso_639_1"]))
            if not cur.fetchone():
                cur.execute("INSERT INTO movie_languages (movie_id, language_code) VALUES (%s, %s);", (movie_id, lang["iso_639_1"]))
                log_update(cur, movie_id, movie["title"], "language_added", "language", None, lang["name"])
                print(f"🗣️ Added spoken language '{lang['name']}' to movie '{movie['title']}'")

        for country in movie.get("production_countries", []):
            cur.execute("INSERT INTO countries (iso_3166_1, name) VALUES (%s, %s) ON CONFLICT DO NOTHING;", (country["iso_3166_1"], country["name"]))
            cur.execute("SELECT 1 FROM movie_countries WHERE movie_id = %s AND country_code = %s;", (movie_id, country["iso_3166_1"]))
            if not cur.fetchone():
                cur.execute("INSERT INTO movie_countries (movie_id, country_code) VALUES (%s, %s);", (movie_id, country["iso_3166_1"]))
                log_update(cur, movie_id, movie["title"], "country_added", "country", None, country["name"])
                print(f"🌍 Added production country '{country['name']}' to movie '{movie['title']}'")

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
        conn.commit()
