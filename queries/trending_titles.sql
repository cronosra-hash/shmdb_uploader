-- sql/trending_titles.sql
SELECT id, title, lastupdated
FROM movies
WHERE lastupdated > NOW() - INTERVAL '7 days'
ORDER BY lastupdated DESC
LIMIT 10;
-- This query retrieves the 10 most recently updated movie titles from the past week.