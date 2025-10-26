SELECT
  COUNT(*) FILTER (
    WHERE
      overview IS NULL
  ) AS missing_overview,
  COUNT(*) FILTER (
    WHERE
      release_date IS NULL
  ) AS missing_release_date,
  COUNT(*) FILTER (
    WHERE
      runtime IS NULL
  ) AS missing_runtime,
  COUNT(*) FILTER (
    WHERE
      poster_path IS NULL
  ) AS missing_poster_path,
  COUNT(*) FILTER (
    WHERE
      original_language IS NULL
  ) AS missing_original_language,
  COUNT(*) FILTER (
    WHERE
      status IS NULL
  ) AS missing_status,
  COUNT(*) FILTER (
    WHERE
      imdb_id IS NULL
  ) AS missing_imdb,
  COUNT(*) FILTER (
    WHERE
      budget IS NULL
  ) AS missing_budget,
  COUNT(*) FILTER (
    WHERE
      revenue IS NULL
  ) AS missing_revenue,
  COUNT(*) FILTER (
    WHERE
      tagline IS NULL OR tagline LIKE 'Tagline%' or tagline = ''
  ) AS missing_tagline
FROM
  movies;