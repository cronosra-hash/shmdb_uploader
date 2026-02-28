SELECT json_build_object(

    -- Top fields
    'top_fields',
    (
        SELECT json_agg(row_to_json(t))
        FROM (
            SELECT field_name, COUNT(*) AS freq
            FROM update_logs
            GROUP BY field_name
            ORDER BY freq DESC
            LIMIT 5
        ) t
    ),

    -- Top rated movies
    'top_rated_movies',
    (
        SELECT json_agg(row_to_json(t))
        FROM (
            SELECT 
                m.movie_id AS id,
                m.movie_title,
                m.vote_average
            FROM movies m
            WHERE m.vote_count > 0
            ORDER BY m.vote_average DESC
            LIMIT 10
        ) t
    ),

    -- Popular genres
    'popular_genres',
    (
        SELECT json_agg(row_to_json(t))
        FROM (
            SELECT 
                g.genre_name AS genre_name,
                COUNT(*) AS genre_count
            FROM genres g
            JOIN movie_genres mg ON mg.genre_id = g.genre_id
            GROUP BY g.genre_name
            ORDER BY genre_count DESC
            LIMIT 10
        ) t
    ),

    -- Hidden gems
    'hidden_gems',
    (
        SELECT json_agg(row_to_json(t))
        FROM (
            SELECT 
                m.movie_id,
                m.movie_title,
                m.vote_average,
                m.vote_count
            FROM movies m
            WHERE m.vote_count > 50 AND m.vote_average >= 7.5
            ORDER BY m.vote_average DESC
            LIMIT 10
        ) t
    ),

    -- Most reviewed titles
    'most_reviewed_titles',
    (
        SELECT json_agg(row_to_json(t))
        FROM (
            SELECT 
                m.movie_id,
                m.movie_title,
                m.vote_count
            FROM movies m
            ORDER BY m.vote_count DESC
            LIMIT 10
        ) t
    ),

    -- Active release years
    'active_release_years',
    (
        SELECT json_agg(row_to_json(t))
        FROM (
            SELECT 
                mm.release_year,
                COUNT(*) AS title_count
            FROM movie_metadata mm
            GROUP BY mm.release_year
            ORDER BY mm.release_year DESC
            LIMIT 20
        ) t
    ),

    -- Trending titles
    'trending_titles',
    (
        SELECT json_agg(row_to_json(t))
        FROM (
            SELECT 
                m.movie_id,
                m.movie_title,
                m.last_updated
            FROM movies m
            ORDER BY m.last_updated DESC
            LIMIT 10
        ) t
    ),

    -- Most updated title
    'most_updated_title',
    (
        SELECT row_to_json(t)
        FROM (
            SELECT 
                COALESCE(m.movie_title, s.series_name) AS most_updated_title,
                CASE 
                    WHEN m.movie_id IS NOT NULL THEN 'movie'
                    ELSE 'tv'
                END AS title_type,
                COUNT(*) AS title_changes
            FROM update_logs ul
            LEFT JOIN movies m ON m.movie_id = ul.content_id
            LEFT JOIN series s ON s.series_id = ul.content_id
            WHERE m.movie_id IS NOT NULL OR s.series_id IS NOT NULL
            GROUP BY most_updated_title, title_type
            ORDER BY title_changes DESC
            LIMIT 1
        ) t
    ),

    -- Movies missing fields (correct table: movies)
    'movies_missing_fields',
    (
        SELECT row_to_json(t)
        FROM (
            SELECT
                SUM(CASE WHEN overview IS NULL THEN 1 ELSE 0 END) AS missing_overview,
                SUM(CASE WHEN release_date IS NULL THEN 1 ELSE 0 END) AS missing_release_date,
                SUM(CASE WHEN runtime IS NULL THEN 1 ELSE 0 END) AS missing_runtime,
                SUM(CASE WHEN poster_path IS NULL THEN 1 ELSE 0 END) AS missing_poster_path,
                SUM(CASE WHEN original_language IS NULL THEN 1 ELSE 0 END) AS missing_original_language,
                SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) AS missing_status,
                SUM(CASE WHEN imdb_id IS NULL THEN 1 ELSE 0 END) AS missing_imdb,
                SUM(CASE WHEN budget IS NULL THEN 1 ELSE 0 END) AS missing_budget,
                SUM(CASE WHEN revenue IS NULL THEN 1 ELSE 0 END) AS missing_revenue,
                SUM(CASE WHEN tagline IS NULL THEN 1 ELSE 0 END) AS missing_tagline
            FROM movies
        ) t
    ),

    -- Series missing fields
    'series_missing_fields',
    (
        SELECT row_to_json(t)
        FROM (
            SELECT
                SUM(CASE WHEN overview IS NULL THEN 1 ELSE 0 END) AS missing_overview,
                SUM(CASE WHEN first_air_date IS NULL THEN 1 ELSE 0 END) AS missing_first_air_date,
                SUM(CASE WHEN poster_path IS NULL THEN 1 ELSE 0 END) AS missing_poster_path,
                SUM(CASE WHEN original_language IS NULL THEN 1 ELSE 0 END) AS missing_original_language,
                SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) AS missing_status,
                SUM(CASE WHEN imdb_id IS NULL THEN 1 ELSE 0 END) AS missing_imdb,
                SUM(CASE WHEN number_of_seasons IS NULL OR number_of_seasons = 0 THEN 1 ELSE 0 END) AS missing_seasons,
                SUM(CASE WHEN number_of_episodes IS NULL OR number_of_episodes = 0 THEN 1 ELSE 0 END) AS missing_episodes
            FROM series
        ) t
    ),

    -- Orphaned logs
    'orphaned_logs',
    (
        SELECT COUNT(*)
        FROM update_logs ul
        LEFT JOIN movies m ON m.movie_id = ul.content_id
        LEFT JOIN series s ON s.series_id = ul.content_id
        WHERE m.movie_id IS NULL AND s.series_id IS NULL
    ),

    -- Movie count
    'movie_count',
    (SELECT COUNT(*) FROM movies),

    -- Series count
    'series_count',
    (SELECT COUNT(*) FROM series),

    -- Episode count
    'episode_count',
    (SELECT COUNT(*) FROM series_episodes),

    -- Season count
    'season_count',
    (SELECT COUNT(*) FROM series_seasons),

    -- Cast and people counts
    'movie_cast_count',
    (SELECT COUNT(*) FROM movie_cast),
    'series_cast_count',
    (SELECT COUNT(*) FROM series_cast),
    'people_count',
    (SELECT COUNT(*) FROM people),

    -- Last update timestamp
    'last_update',
    (
        SELECT MAX("timestamp")::timestamptz
        FROM update_logs
    ),

    -- Freshness stats (movies + series)
    'freshness',
    (
        SELECT row_to_json(t)
        FROM (
            SELECT
                SUM(CASE WHEN last_updated >= NOW() - INTERVAL '1 day' THEN 1 ELSE 0 END) AS fresh,
                SUM(CASE WHEN last_updated < NOW() - INTERVAL '1 day'
                        AND last_updated >= NOW() - INTERVAL '7 days' THEN 1 ELSE 0 END) AS stale,
                SUM(CASE WHEN last_updated < NOW() - INTERVAL '7 days' THEN 1 ELSE 0 END) AS old
            FROM (
                SELECT last_updated FROM movies
                UNION ALL
                SELECT last_updated FROM series
            ) u
        ) t
    )


) AS stats;
