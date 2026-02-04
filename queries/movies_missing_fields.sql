SELECT
  COUNT(*) FILTER (
    WHERE overview IS NULL OR overview = ''
  ) AS missing_overview,

  COUNT(*) FILTER (
    WHERE release_date IS NULL
  ) AS missing_release_date,

  COUNT(*) FILTER (
    WHERE runtime IS NULL
  ) AS missing_runtime,

  COUNT(*) FILTER (
    WHERE poster_path IS NULL OR poster_path = ''
  ) AS missing_poster_path,

  COUNT(*) FILTER (
    WHERE original_language IS NULL OR original_language = ''
  ) AS missing_original_language,

  COUNT(*) FILTER (
    WHERE status IS NULL OR status = ''
  ) AS missing_status,

  COUNT(*) FILTER (
    WHERE imdb_id IS NULL OR imdb_id = ''
  ) AS missing_imdb,

  COUNT(*) FILTER (
    WHERE budget IS NULL
  ) AS missing_budget,

  COUNT(*) FILTER (
    WHERE revenue IS NULL
  ) AS missing_revenue,

  COUNT(*) FILTER (
    WHERE tagline IS NULL
  ) AS missing_tagline
FROM movies;
