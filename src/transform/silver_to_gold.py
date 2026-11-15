"""Silver -> Gold for one month: fact_zone_month(year_month, taxi_type, pickup_location_id, trips, shared_trips)."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from job_utils import parse_args, resolve_year_month
from lake import build_spark, read_silver_month, write_month


def fact_zone_month(silver: DataFrame, year_month: str) -> DataFrame:
    """Trips (H1-H5) and shared trips (H3) per type and pickup zone; null zone kept as its own row (H5)."""
    return (
        silver.groupBy("taxi_type", "pickup_location_id")
        .agg(F.count(F.lit(1)).alias("trips"), F.sum(F.col("is_shared").cast("long")).alias("shared_trips"))
        .select(F.lit(year_month).alias("year_month"), "taxi_type", "pickup_location_id", "trips", "shared_trips")
    )


def main() -> None:
    args = parse_args(["BUCKET", "YEAR_MONTH", "TABLE_FORMAT"])
    bucket = args["BUCKET"]
    year_month = resolve_year_month(bucket, args["YEAR_MONTH"])
    spark = build_spark("nyc-taxi-silver-to-gold", bucket)
    silver = read_silver_month(spark, table_format=args["TABLE_FORMAT"], bucket=bucket, year_month=year_month)
    write_month(spark, fact_zone_month(silver, year_month), "fact_zone_month", table_format=args["TABLE_FORMAT"],
                bucket=bucket, year_month=year_month, iceberg_partitions="year_month")


if __name__ == "__main__":
    main()
