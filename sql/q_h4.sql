-- H4: DQ does not distort the answer: raw footer rows vs rows written to Silver vs Gold trips, per month and type.
WITH gold AS (
  SELECT year_month, taxi_type, SUM(trips) AS gold_trips
  FROM nyc_taxi.fact_zone_month
  GROUP BY year_month, taxi_type
)
SELECT d.year_month, d.taxi_type, d.source_rows_footer, d.read_rows, d.written_rows, g.gold_trips,
       d.source_rows_footer - d.written_rows AS removed_rows,
       d.reject_rate, d.status,
       g.gold_trips = d.written_rows AS gold_matches_silver
FROM nyc_taxi.dq_reports d
LEFT JOIN gold g ON g.year_month = d.year_month AND g.taxi_type = d.taxi_type
WHERE d.year_month >= '2019-02'
ORDER BY d.year_month, d.taxi_type
