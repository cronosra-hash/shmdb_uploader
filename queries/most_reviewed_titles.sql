-- sql/most_reviewed_titles.sql
SELECT movie_id, movie_title, vote_count
FROM movies
WHERE vote_count IS NOT NULL
ORDER BY vote_count DESC
LIMIT 10;
-- This query finds the top 10 most reviewed movies based on vote_count.