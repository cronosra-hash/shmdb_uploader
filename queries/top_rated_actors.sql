-- sql/top_rated_actors.sql
SELECT c.actor_name, ROUND(AVG(t.rating), 2) AS avg_rating, COUNT(*) AS title_count
FROM cast c
JOIN titles t ON c.title_id = t.title_id
WHERE t.rating IS NOT NULL
GROUP BY c.actor_name
HAVING COUNT(*) > 5
ORDER BY avg_rating DESC
LIMIT 10;
