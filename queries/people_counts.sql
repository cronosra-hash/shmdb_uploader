SELECT 
    p.name AS actor_name,

    -- Movies: one per movie
    COUNT(DISTINCT mc.movie_id) AS movie_appearances,

    -- TV: one per series
    COUNT(DISTINCT sc.series_id) AS tv_appearances,

    -- TV Episodes
    COUNT(sc.series_id) AS tv_episodes,

    -- Combined
    COUNT(DISTINCT mc.movie_id)
        + COUNT(DISTINCT sc.series_id) AS combined_appearances

FROM people p
LEFT JOIN movie_cast mc 
    ON mc.actor_id = p.person_id
LEFT JOIN series_cast sc 
    ON sc.person_id = p.person_id

GROUP BY p.name
ORDER BY combined_appearances DESC
LIMIT 20;
-- This query retrieves the top 20 actors with the most appearances in movies and TV shows, counting movies and TV series separately.