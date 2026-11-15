-- 25 Silver rows per type for samples/silver_sample_100.csv.
SELECT * FROM (SELECT * FROM nyc_taxi.trips_clean WHERE taxi_type = 'yellow' LIMIT 25)
UNION ALL SELECT * FROM (SELECT * FROM nyc_taxi.trips_clean WHERE taxi_type = 'green' LIMIT 25)
UNION ALL SELECT * FROM (SELECT * FROM nyc_taxi.trips_clean WHERE taxi_type = 'fhv' LIMIT 25)
UNION ALL SELECT * FROM (SELECT * FROM nyc_taxi.trips_clean WHERE taxi_type = 'hvfhv' LIMIT 25)
