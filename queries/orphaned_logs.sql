SELECT
    COUNT(*) AS orphaned_logs
FROM
    update_logs
WHERE
    content_id NOT IN (
        SELECT
            movie_id
        FROM
            movies
        UNION
        SELECT
            series_id
        FROM
            series
    );