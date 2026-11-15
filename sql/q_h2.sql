-- H2: leading type per month (all types and taxi-only); a change of leader shows when it happened.
WITH monthly AS (
  SELECT year_month, taxi_type, SUM(trips) AS trips
  FROM nyc_taxi.fact_zone_month
  WHERE year_month >= '2019-02'
  GROUP BY year_month, taxi_type
),
ranked AS (
  SELECT year_month, taxi_type, trips,
         RANK() OVER (PARTITION BY year_month ORDER BY trips DESC) AS rank_all,
         RANK() OVER (PARTITION BY year_month, taxi_type IN ('yellow', 'green') ORDER BY trips DESC) AS rank_in_group
  FROM monthly
)
SELECT year_month,
       MAX(CASE WHEN rank_all = 1 THEN taxi_type END) AS leader_all_types,
       MAX(CASE WHEN rank_in_group = 1 AND taxi_type IN ('yellow', 'green') THEN taxi_type END) AS leader_taxi_only,
       SUM(CASE WHEN taxi_type = 'yellow' THEN trips END) AS yellow_trips,
       SUM(CASE WHEN taxi_type = 'green' THEN trips END) AS green_trips,
       SUM(CASE WHEN taxi_type = 'fhv' THEN trips END) AS fhv_trips,
       SUM(CASE WHEN taxi_type = 'hvfhv' THEN trips END) AS hvfhv_trips
FROM ranked
GROUP BY year_month
ORDER BY year_month
