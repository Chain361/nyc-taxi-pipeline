import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, TimestampType, StringType

# Read Glue Job Arguments
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "S3_BUCKET_NAME"]
)
BUCKET_NAME = args["S3_BUCKET_NAME"]

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("NYCTaxi-Bronze-to-Silver") \
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

BRONZE_BASE_PATH = f"s3://{BUCKET_NAME}/bronze"
SILVER_OUTPUT_PATH = f"s3://{BUCKET_NAME}/silver"

def process_taxi_data(taxi_type: str, pickup_col: str, dropoff_col :str):
    "Reads raw Bronze parquet, harmonizes schema, cleans anomalies, and returns a DataFrame."
    input_path = f"{BRONZE_BASE_PATH}/taxi_type={taxi_type}/year=2024/*.parquet"

    df = spark.read.parquet(input_path)

    # 1. Harmonize timestamps and identify taxi type
    df = df.withColumn("pickup_datetime", F.col(pickup_col).cast(TimestampType())) \
        .withColumn("dropoff_datetime", F.col(dropoff_col).cast(TimestampType())) \
        .withColumn("taxi_type", F.lit(taxi_type).cast(StringType()))
    
    # 2. Add columns unique to Yellow or Green if missing
    if "airport_fee" not in df.columns:
        df = df.withColumn("airport_fee", F.lit(0.0).cast(DoubleType()))
    else:
        df = df.withColumn("airport_fee", F.col("airport_fee").cast(DoubleType()))

    if "trip_type" not in df.columns:
        df = df.withColumn("trip_type", F.lit(1).cast(IntegerType()))  # 1 = Street-hail
    else:
        df = df.withColumn("trip_type", F.col("trip_type").cast(IntegerType()))

    if "cbd_congestion_fee" not in df.columns:
        df = df.withColumn("cbd_congestion_fee", F.lit(0.0).cast(DoubleType()))
    else:
        df = df.withColumn("cbd_congestion_fee", F.col("cbd_congestion_fee").cast(DoubleType()))

    # 3. Standardize types and column names
    standardized_df = df.select(
        F.col("taxi_type"),
        F.col("VendorID").cast(IntegerType()).alias("vendor_id"),
        F.col("pickup_datetime"),
        F.col("dropoff_datetime"),
        F.col("passenger_count").cast(IntegerType()).alias("passenger_count"),
        F.col("trip_distance").cast(DoubleType()).alias("trip_distance"),
        F.col("RatecodeID").cast(IntegerType()).alias("rate_code_id"),
        F.col("store_and_fwd_flag").cast(StringType()).alias("store_and_fwd_flag"),
        F.col("PULocationID").cast(IntegerType()).alias("pickup_location_id"),
        F.col("DOLocationID").cast(IntegerType()).alias("dropoff_location_id"),
        F.col("payment_type").cast(IntegerType()).alias("payment_type"),
        F.col("fare_amount").cast(DoubleType()).alias("fare_amount"),
        F.col("extra").cast(DoubleType()).alias("extra"),
        F.col("mta_tax").cast(DoubleType()).alias("mta_tax"),
        F.col("tip_amount").cast(DoubleType()).alias("tip_amount"),
        F.col("tolls_amount").cast(DoubleType()).alias("tolls_amount"),
        F.col("improvement_surcharge").cast(DoubleType()).alias("improvement_surcharge"),
        F.col("congestion_surcharge").cast(DoubleType()).alias("congestion_surcharge"),
        F.col("airport_fee"),
        F.col("cbd_congestion_fee"),
        F.col("total_amount").cast(DoubleType()).alias("total_amount"),
        F.col("trip_type")
    )

    # 4. Filter data anomalies (Cleansing Rules)
    cleaned_df = standardized_df.filter(
        # Date boundaries for H1 2024
        (F.col("pickup_datetime") >= "2024-01-01 00:00:00") &
        (F.col("pickup_datetime") < "2024-07-01 00:00:00") &
        (F.col("dropoff_datetime") > F.col("pickup_datetime")) &
        # Physical metrics validation
        (F.col("passenger_count") > 0) &
        (F.col("trip_distance") > 0.0) &
        # Financial validation
        (F.col("fare_amount") >= 0.0) &
        (F.col("total_amount") >= 0.0)
    ).dropDuplicates()

    # 5. Extract partition columns
    final_df = cleaned_df.withColumn("year", F.year("pickup_datetime")) \
                         .withColumn("month", F.date_format("pickup_datetime", "MM"))

    return final_df

# Process Yellow and Green records
yellow_clean = process_taxi_data("yellow", "tpep_pickup_datetime", "tpep_dropoff_datetime")
green_clean = process_taxi_data("green", "lpep_pickup_datetime", "lpep_dropoff_datetime")

# Union datasets into unified Silver schema
silver_df = yellow_clean.unionByName(green_clean)

# Write to S3 Silver Layer partitioned by year and month
silver_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(f"{SILVER_OUTPUT_PATH}/trips_clean/")

spark.stop()
    


    