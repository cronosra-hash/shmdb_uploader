-- sql/prolific_actors.sql
SELECT p.name AS actor_name, COUNT(*) AS appearances
FROM moviecast mc
JOIN people p ON mc.person_id = p.person_id
GROUP BY p.name
ORDER BY appearances DESC
LIMIT 10;
-- This query retrieves the top 10 actors with the most appearances in movies.