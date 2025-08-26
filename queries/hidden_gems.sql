-- sql/hidden_gems.sql
SELECT title_id, title_name, rating, review_count
FROM titles
WHERE rating >= 8.0 AND review_count < 10
ORDER BY rating DESC
LIMIT 10;
