-- sql/trending_titles.sql
SELECT title_id, title_name, updated_at
FROM titles
WHERE updated_at > NOW() - INTERVAL '7 days'
ORDER BY updated_at DESC
LIMIT 10;
