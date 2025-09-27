SELECT
    COUNT(*) FILTER (
        WHERE
            overview IS NULL
    ) AS missing_overview,
    COUNT(*) FILTER (
        WHERE
            first_air_date IS NULL
    ) AS missing_first_air_date,
    COUNT(*) FILTER (
        WHERE
            poster_path IS NULL
    ) AS missing_poster_path,
    COUNT(*) FILTER (
        WHERE
            original_language IS NULL
    ) AS missing_original_language,
    COUNT(*) FILTER (
        WHERE
            status IS NULL
    ) AS missing_status,
    COUNT(*) FILTER (
        WHERE
            imdb_id IS NULL
    ) AS missing_imdb,
    COUNT(*) FILTER (
        WHERE
            number_of_seasons IS NULL
    ) AS missing_seasons,
    COUNT(*) FILTER (
        WHERE
            number_of_episodes IS NULL
    ) AS missing_episodes
FROM
    series;