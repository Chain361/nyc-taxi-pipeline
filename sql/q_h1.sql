-- H1: type with the most total trips, Feb 2019 -> latest month (taxi-only and all types).
WITH totals AS (
  SELECT taxi_type, SUM(trips) AS trips, MIN(year_month) AS first_month, MAX(year_month) AS last_month
  FROM nyc_taxi.fact_zone_month
  WHERE year_month >= '2019-02'
  GROUP BY taxi_type
),
views AS (
  SELECT 'taxi_only' AS view, * FROM totals WHERE taxi_type IN ('yellow', 'green')
  UNION ALL
  SELECT 'all_types' AS view, * FROM totals
)
SELECT view, taxi_type, trips, first_month, last_month,
       ROUND(100.0 * trips / SUM(trips) OVER (PARTITION BY view), 2) AS share_pct,
       RANK() OVER (PARTITION BY view ORDER BY trips DESC) AS rank
FROM views
ORDER BY view, rank
