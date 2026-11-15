-- DQ summary per type over all months; null = rule not applicable to the type.
SELECT taxi_type,
       COUNT(*) AS months,
       SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed_months,
       SUM(source_rows_footer) AS source_rows_footer,
       SUM(read_rows) AS read_rows,
       SUM(written_rows) AS written_rows,
       SUM(rejects.out_of_month) AS out_of_month,
       SUM(rejects.dropoff_before_pickup) AS dropoff_before_pickup,
       SUM(rejects.negative_amount) AS negative_amount,
       SUM(rejects.negative_distance) AS negative_distance,
       SUM(flags.passenger_count_invalid) AS passenger_count_invalid,
       SUM(flags.pickup_location_null) AS pickup_location_null,
       SUM(flags.shared) AS shared,
       ROUND(1 - CAST(SUM(written_rows) AS double) / SUM(read_rows), 6) AS reject_rate
FROM nyc_taxi.dq_reports
WHERE year_month >= '2019-02'
GROUP BY taxi_type
ORDER BY taxi_type
