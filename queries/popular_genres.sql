-- sql/popular_genres.sql
SELECT genre, COUNT(*) AS genre_count
FROM titles
WHERE genre IS NOT NULL
GROUP BY genre
ORDER BY genre_count DESC
LIMIT 10;
