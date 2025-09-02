-- sql/trending_titles.sql
SELECT movie_id, movie_title, last_updated
FROM movies
WHERE last_updated > NOW() - INTERVAL '7 days'
ORDER BY last_updated DESC
LIMIT 10;
-- This query retrieves the 10 most recently updated movie titles from the past week.