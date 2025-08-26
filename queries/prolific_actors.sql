-- sql/prolific_actors.sql
SELECT actor_name, COUNT(*) AS appearances
FROM cast
GROUP BY actor_name
ORDER BY appearances DESC
LIMIT 10;
