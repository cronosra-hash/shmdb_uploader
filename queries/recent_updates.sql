SELECT
    COUNT(*) AS recent_updates
FROM
    update_logs
WHERE
    timestamp >= NOW () - INTERVAL '7 days';