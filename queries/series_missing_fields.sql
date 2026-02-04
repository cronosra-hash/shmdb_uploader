SELECT
    COUNT(*) FILTER (
        WHERE overview IS NULL OR overview = ''
    ) AS missing_overview,

    COUNT(*) FILTER (
        WHERE first_air_date IS NULL
    ) AS missing_first_air_date,

    COUNT(*) FILTER (
        WHERE poster_path IS NULL OR poster_path = ''
    ) AS missing_poster_path,

    COUNT(*) FILTER (
        WHERE original_language IS NULL OR original_language = ''
    ) AS missing_original_language,

    COUNT(*) FILTER (
        WHERE status IS NULL OR status = ''
    ) AS missing_status,

    COUNT(*) FILTER (
        WHERE imdb_id IS NULL OR imdb_id = ''
    ) AS missing_imdb,

    COUNT(*) FILTER (
        WHERE number_of_seasons IS NULL
    ) AS missing_seasons,

    COUNT(*) FILTER (
        WHERE number_of_episodes IS NULL
    ) AS missing_episodes
FROM series;
