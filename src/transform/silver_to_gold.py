import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Read Glue Job Arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET_NAME"])
BUCKET_NAME = args["S3_BUCKET_NAME"]

# Initialize Spark session
spark = SparkSession.builder \
    .appName("NYCTaxi-Silver-to-Gold") \
        .getOrCreate()

SILVER_INPUT_PATH = f"s3://{BUCKET_NAME}/silver/trips_clean/"
LOOKUP_INPUT_PATH = f"s3://{BUCKET_NAME}/bronze/lookup/taxi_zone_lookup.csv"
GOLD_OUTPUT_BASE = f"s3://{BUCKET_NAME}/gold"

# 1. Read Silver Dataset and Zone Lookup
silver_df = spark.read.parquet(SILVER_INPUT_PATH)

lookup_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(LOOKUP_INPUT_PATH) \
    .select(
        F.col("LocationID").alias("location_id"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone"),
        F.col("service_zone").alias("pickup_service_zone")
    )

# 2.Join pickup borough with silver trips
enriched_df = silver_df.join(
    lookup_df,
    silver_df["pickup_location_id"] == lookup_df["location_id"],
    how="left"
).fillna({"pickup_borough": "Unknown", "pickup_zone": "Unknown"})

# -------------------------------------------------------------
# TABLE 1: Monthly KPI Summary (Direct Answer to H1 2024 Comparison)
# -------------------------------------------------------------
monthly_kpi_df = enriched_df.groupBy("year", "month", "taxi_type").agg(
    F.count("*").alias("total_trips"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    F.round(F.avg("total_amount"), 2).alias("avg_total_fare"),
    F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
    F.round(F.sum("tip_amount"), 2).alias("total_tips"),
    F.round(
        F.when(F.sum("fare_amount") > 0, (F.sum("tip_amount") / F.sum("fare_amount")) * 100).otherwise(0.0),
        2
    ).alias("tip_percentage")
)

# write Monthly KPI partitioned by year
monthly_kpi_df.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet(f"{GOLD_OUTPUT_BASE}/monthly_kpi/")

# -------------------------------------------------------------
# TABLE 2: Zone & Borough Demand Performance
# -------------------------------------------------------------

zone_performance_df = enriched_df.groupBy("year", "taxi_type", "pickup_borough", "pickup_zone").agg(
    F.count("*").alias("total_pickups"),
    F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    F.round(F.avg("trip_distance"), 2).alias("avg_distance")
)

# Write Zone Demand partitioned by year and taxi_type
zone_performance_df.write \
    .mode("overwrite") \
    .partitionBy("year", "taxi_type") \
    .parquet(f"{GOLD_OUTPUT_BASE}/zone_performance/")

spark.stop()    