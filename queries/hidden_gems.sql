-- sql/hidden_gems.sql
SELECT id, title, vote_average, vote_count
FROM movies
WHERE vote_average >= 8.0 AND vote_count < 10
ORDER BY vote_average DESC
LIMIT 10;
-- This query finds highly rated movies (vote_average >= 8.0) that have received relatively few votes (vote_count < 10).