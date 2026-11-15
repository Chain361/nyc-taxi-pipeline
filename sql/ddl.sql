-- Iceberg tables trips_clean, fact_zone_month, dim_zone are created by the Glue jobs (CREATE TABLE IF NOT EXISTS).
-- This file registers the DQ reports (JSON written by bronze_to_silver) for Athena.
-- <BUCKET> is replaced by scripts/export_samples.py (or by hand in the Athena console).
-- Partition projection: no MSCK REPAIR needed.
CREATE EXTERNAL TABLE IF NOT EXISTS nyc_taxi.dq_reports (
  source_rows_footer bigint,
  read_rows bigint,
  rejects struct<out_of_month:bigint, dropoff_before_pickup:bigint, negative_amount:bigint, negative_distance:bigint>,
  flags struct<passenger_count_invalid:bigint, pickup_location_null:bigint, shared:bigint>,
  written_rows bigint,
  reject_rate double,
  status string,
  run_id string,
  generated_at string
)
PARTITIONED BY (year_month string, taxi_type string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://<BUCKET>/dq_reports/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.year_month.type' = 'date',
  'projection.year_month.format' = 'yyyy-MM',
  'projection.year_month.range' = '2019-02,NOW',
  'projection.year_month.interval' = '1',
  'projection.year_month.interval.unit' = 'MONTHS',
  'projection.taxi_type.type' = 'enum',
  'projection.taxi_type.values' = 'yellow,green,fhv,hvfhv',
  'storage.location.template' = 's3://<BUCKET>/dq_reports/year_month=${year_month}/taxi_type=${taxi_type}/'
)
