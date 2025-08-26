-- sql/top_rated_movies.sql
SELECT title_id, title_name, rating
FROM titles
WHERE type = 'movie' AND rating IS NOT NULL
ORDER BY rating DESC
LIMIT 10;
