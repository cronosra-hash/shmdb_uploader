-- sql/top_rated_movies.sql
SELECT movie_id, movie_title, vote_average
FROM movies
WHERE vote_average IS NOT NULL
ORDER BY vote_average DESC
LIMIT 10;
-- This query retrieves the top 10 movies with the highest average ratings.