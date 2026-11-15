"""Zone dimension: taxi_zone_lookup.csv -> dim_zone (full overwrite); GeoJSON copied to gold/geo/ for the map."""
from __future__ import annotations

import boto3
from pyspark.sql import functions as F

from job_utils import parse_args
from lake import build_spark, write_full

LOOKUP_KEY = "bronze/lookup/taxi_zone_lookup.csv"
GEOJSON_SOURCE_KEY = "bronze/geo/taxi_zones.geojson"
GEOJSON_GOLD_KEY = "gold/geo/taxi_zones.geojson"


def main() -> None:
    args = parse_args(["BUCKET", "TABLE_FORMAT"])
    bucket = args["BUCKET"]
    spark = build_spark("nyc-taxi-dim-zone", bucket)
    dim = spark.read.option("header", "true").csv(f"s3://{bucket}/{LOOKUP_KEY}").select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone").alias("service_zone"),
    )
    write_full(spark, dim, "dim_zone", table_format=args["TABLE_FORMAT"], bucket=bucket)
    boto3.client("s3").copy_object(Bucket=bucket, Key=GEOJSON_GOLD_KEY,
                                   CopySource={"Bucket": bucket, "Key": GEOJSON_SOURCE_KEY})


if __name__ == "__main__":
    main()
