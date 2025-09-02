-- sql/active_release_years.sql
SELECT mmd.release_year, COUNT(*) AS title_count
FROM movies m
JOIN movie_metadata mmd ON mmd.movie_id = m.movie_id
GROUP BY mmd.release_year
ORDER BY title_count DESC
LIMIT 10;
