-- H5: leading type per pickup zone over H1 2024 (all types and taxi-only).
-- Rows with null pickup_location_id (frequent in fhv) are excluded here; their count is in q_dq_summary.
WITH zone_type AS (
  SELECT pickup_location_id, taxi_type, SUM(trips) AS trips
  FROM nyc_taxi.fact_zone_month
  WHERE year_month BETWEEN '2024-01' AND '2024-06' AND pickup_location_id IS NOT NULL
  GROUP BY pickup_location_id, taxi_type
),
ranked AS (
  SELECT 'all_types' AS view, *, RANK() OVER (PARTITION BY pickup_location_id ORDER BY trips DESC) AS rank
  FROM zone_type
  UNION ALL
  SELECT 'taxi_only' AS view, *, RANK() OVER (PARTITION BY pickup_location_id ORDER BY trips DESC) AS rank
  FROM zone_type WHERE taxi_type IN ('yellow', 'green')
)
SELECT r.view, r.pickup_location_id, z.borough, z.zone, r.taxi_type AS leading_type, r.trips
FROM ranked r
LEFT JOIN nyc_taxi.dim_zone z ON z.location_id = r.pickup_location_id
WHERE r.rank = 1
ORDER BY r.view, z.borough, z.zone
