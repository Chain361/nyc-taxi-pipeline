"""Spark session + table read/write for Iceberg (default) or the plain-parquet fallback."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from taxi_types import month_bounds

CATALOG = "glue"
DATABASE = "nyc_taxi"


def build_spark(app_name: str, bucket: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", f"s3://{bucket}/warehouse/")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .getOrCreate()
    )


def _table(name: str) -> str:
    return f"{CATALOG}.{DATABASE}.{name}"


def _parquet_path(bucket: str, name: str) -> str:
    return f"s3://{bucket}/parquet/{name}/"


def _ensure_iceberg_table(spark: SparkSession, df: DataFrame, name: str, partition_sql: str | None) -> None:
    columns = ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in df.schema.fields)
    partitioned = f"PARTITIONED BY ({partition_sql})" if partition_sql else ""
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {_table(name)} ({columns}) USING iceberg {partitioned} "
        "TBLPROPERTIES ('format-version'='2', 'commit.retry.num-retries'='10')"
    )


def write_month(spark: SparkSession, df: DataFrame, name: str, *, table_format: str, bucket: str,
                year_month: str, iceberg_partitions: str) -> None:
    """Idempotently replace only this month's partitions (df must hold one month only)."""
    if table_format == "iceberg":
        _ensure_iceberg_table(spark, df, name, iceberg_partitions)
        df.writeTo(_table(name)).overwritePartitions()
        return
    out = df if "year_month" in df.columns else df.withColumn("year_month", F.lit(year_month))
    out.write.mode("overwrite").partitionBy("year_month", "taxi_type").parquet(_parquet_path(bucket, name))


def write_full(spark: SparkSession, df: DataFrame, name: str, *, table_format: str, bucket: str) -> None:
    if table_format == "iceberg":
        _ensure_iceberg_table(spark, df, name, None)
        df.writeTo(_table(name)).overwritePartitions()  # unpartitioned table: replaces all rows atomically
        return
    df.write.mode("overwrite").parquet(_parquet_path(bucket, name))


def read_silver_month(spark: SparkSession, *, table_format: str, bucket: str, year_month: str) -> DataFrame:
    start, end = month_bounds(year_month)
    if table_format == "iceberg":
        df = spark.table(_table("trips_clean"))
    else:
        df = spark.read.parquet(_parquet_path(bucket, "trips_clean")).where(F.col("year_month") == year_month)
    return df.where((F.col("pickup_datetime") >= F.lit(start)) & (F.col("pickup_datetime") < F.lit(end)))
