-- sql/active_release_years.sql
SELECT mad.release_year, COUNT(*) AS title_count
FROM movies m
JOIN movie_additional_details mad ON mad.movie_id = m.id
GROUP BY mad.release_year
ORDER BY title_count DESC
LIMIT 10;
