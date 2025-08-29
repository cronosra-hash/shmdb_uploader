-- sql/top_rated_actors.sql
SELECT p.name AS actor_name,
       ROUND(AVG(m.vote_average)::numeric, 2) AS avg_rating,
       COUNT(*) AS title_count
FROM moviecast mc
JOIN people p ON mc.person_id = p.person_id
JOIN movies m ON mc.movie_id = m.id
WHERE m.vote_average IS NOT NULL
GROUP BY p.name
HAVING COUNT(*) > 5
ORDER BY avg_rating DESC
LIMIT 10;
-- This query retrieves the top 10 actors with the highest average movie ratings, considering only those who have appeared in more than 5 movies.
