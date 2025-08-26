-- sql/most_reviewed_titles.sql
SELECT title_id, title_name, review_count
FROM titles
WHERE review_count IS NOT NULL
ORDER BY review_count DESC
LIMIT 10;
