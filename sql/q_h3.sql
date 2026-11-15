-- H3: robustness of the leader to definitions: taxi-only vs all types, with vs without shared/pooled trips.
WITH totals AS (
  SELECT taxi_type, SUM(trips) AS trips, SUM(trips - shared_trips) AS trips_excl_shared
  FROM nyc_taxi.fact_zone_month
  WHERE year_month BETWEEN '2024-01' AND '2024-06'
  GROUP BY taxi_type
),
definitions AS (
  SELECT 'all_types' AS scope, 'with_shared' AS shared, taxi_type, trips FROM totals
  UNION ALL SELECT 'all_types', 'excl_shared', taxi_type, trips_excl_shared FROM totals
  UNION ALL SELECT 'taxi_only', 'with_shared', taxi_type, trips FROM totals WHERE taxi_type IN ('yellow', 'green')
  UNION ALL SELECT 'taxi_only', 'excl_shared', taxi_type, trips_excl_shared FROM totals WHERE taxi_type IN ('yellow', 'green')
)
SELECT scope, shared, taxi_type, trips,
       RANK() OVER (PARTITION BY scope, shared ORDER BY trips DESC) AS rank
FROM definitions
ORDER BY scope, shared, rank
