SELECT field_name, COUNT(*) AS freq
FROM update_logs
GROUP BY field_name
ORDER BY freq DESC
LIMIT 5;