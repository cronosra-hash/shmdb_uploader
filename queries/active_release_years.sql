-- sql/active_release_years.sql
SELECT release_year, COUNT(*) AS title_count
FROM titles
GROUP BY release_year
ORDER BY title_count DESC
LIMIT 10;
