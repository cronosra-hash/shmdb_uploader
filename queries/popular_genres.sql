-- sql/popular_genres.sql
SELECT g.name AS genre, COUNT(*) AS genre_count
FROM genres g
JOIN movie_genres mg ON mg.genre_id = g.id
GROUP BY g.name
ORDER BY genre_count DESC
LIMIT 10;
-- This query retrieves the top 10 most popular movie genres based on the number of associated movies.