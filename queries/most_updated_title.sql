SELECT
    most_updated_title,
    title_type,
    title_changes
FROM
    (
        SELECT
            m.movie_title AS most_updated_title,
            'movie' AS title_type,
            COUNT(*) AS title_changes
        FROM
            update_logs u
            JOIN movies m ON u.content_id = m.movie_id
        GROUP BY
            m.movie_title
        UNION ALL
        SELECT
            s.series_name AS most_updated_title,
            'series' AS title_type,
            COUNT(*) AS title_changes
        FROM
            update_logs u
            JOIN series s ON u.content_id = s.series_id
        GROUP BY
            s.series_name
    ) AS combined
ORDER BY
    title_changes DESC
LIMIT
    1;