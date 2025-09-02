-- hero_stats.sql
SELECT
  COUNT(*) AS total_titles,
  COUNT(*) FILTER (WHERE updated_at > NOW() - INTERVAL '7 days') AS recent_updates,
  ROUND(100.0 * COUNT(*) FILTER (WHERE updated_at > NOW() - INTERVAL '7 days') / COUNT(*), 1) AS freshness_pct,
  COUNT(*) FILTER (WHERE genre IS NULL OR cast IS NULL) AS orphaned_records,
  ROUND(AVG(field_volatility), 2) AS avg_field_volatility
FROM titles;
