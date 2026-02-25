-- sql/top_rated_movies.sql
SELECT
    m.movie_id AS id,
    m.movie_title AS movie_title,
    m.vote_average AS vote_average
FROM movies m
WHERE m.vote_count > 0
ORDER BY m.vote_average DESC
LIMIT 10;
-- This query retrieves the top 10 movies with the highest average ratings.