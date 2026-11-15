"""Bronze -> Silver for one month: per-type canonical cast, month filter, DQ rejects/flags, DQ report."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from job_utils import parse_args, parse_taxi_types, read_json, resolve_year_month, write_json
from lake import build_spark, write_month
from taxi_types import (CANONICAL_COLUMNS, FLAG_RULES, NEGATIVE_AMOUNT_COLUMNS, REJECT_RULES, SHARED_FLAG,
                        bronze_prefix, dq_report_key, manifest_key, month_bounds, resolve_columns, rule_applies)


def to_canonical(raw: DataFrame, taxi_type: str, source_file: str) -> DataFrame:
    """Select/cast the file's columns into the canonical Silver schema; unmapped columns become null."""
    print(f"[COLUMNS] {taxi_type} {source_file}: {raw.columns}")
    resolved = resolve_columns(taxi_type, raw.columns)
    columns = [
        (F.col(src) if src else F.lit(None)).cast(spark_type).alias(canon)
        for canon, spark_type in CANONICAL_COLUMNS.items()
        for src in [resolved[canon]]
    ]
    return raw.select(
        F.lit(taxi_type).alias("taxi_type"),
        *columns,
        shared_expr(taxi_type, raw.columns).alias("is_shared"),
        F.lit(source_file).alias("source_file"),
        F.current_timestamp().alias("ingested_at"),
    )


def shared_expr(taxi_type: str, file_columns: list[str]) -> Column:
    by_lower = {c.lower(): c for c in file_columns}
    column, values = SHARED_FLAG.get(taxi_type, (None, ()))
    src = by_lower.get(column.lower()) if column else None
    if src is None:
        return F.lit(False)
    return F.coalesce(F.upper(F.trim(F.col(src).cast("string"))).isin(*values), F.lit(False))


def reject_conditions(taxi_type: str, year_month: str) -> dict[str, Column]:
    """Applicable reject rules -> boolean column (True = reject). Null-safe."""
    start, end = month_bounds(year_month)
    pickup, dropoff = F.col("pickup_datetime"), F.col("dropoff_datetime")
    rules = {  # lambdas: only applicable rules are built (fhv has no amount columns)
        "out_of_month": lambda: F.coalesce(~((pickup >= F.lit(start)) & (pickup < F.lit(end))), F.lit(True)),
        "dropoff_before_pickup": lambda: F.coalesce(dropoff <= pickup, F.lit(False)),
        "negative_amount": lambda: F.coalesce(
            reduce(Column.__or__, [F.col(c) < 0 for c in NEGATIVE_AMOUNT_COLUMNS[taxi_type]]), F.lit(False)),
        "negative_distance": lambda: F.coalesce(F.col("trip_distance") < 0, F.lit(False)),
    }
    return {name: rules[name]() for name in REJECT_RULES if rule_applies(name, taxi_type)}


def flag_conditions(taxi_type: str) -> dict[str, Column]:
    rules = {
        "passenger_count_invalid": F.coalesce(F.col("passenger_count") <= 0, F.lit(True)),
        "pickup_location_null": F.col("pickup_location_id").isNull(),
        "shared": F.col("is_shared"),
    }
    return {name: rules[name] for name in FLAG_RULES if rule_applies(name, taxi_type)}


def count_rules(df: DataFrame, rejects: dict[str, Column], flags: dict[str, Column]) -> dict[str, int]:
    """One pass: total rows, rows per rule, and rows kept (no reject rule hit)."""
    any_reject = reduce(Column.__or__, rejects.values())
    aggs = [F.count(F.lit(1)).alias("read_rows"), F.sum((~any_reject).cast("long")).alias("written_rows")]
    aggs += [F.sum(cond.cast("long")).alias(f"r_{name}") for name, cond in rejects.items()]
    aggs += [F.sum(cond.cast("long")).alias(f"f_{name}") for name, cond in flags.items()]
    row = df.agg(*aggs).first().asDict()
    return {k: int(v or 0) for k, v in row.items()}


def build_report(taxi_type: str, year_month: str, footer_rows: int, counts: dict[str, int],
                 max_reject_rate: float, run_id: str) -> dict:
    read_rows, written_rows = counts["read_rows"], counts["written_rows"]
    reject_rate = (read_rows - written_rows) / read_rows if read_rows else 1.0
    passed = written_rows > 0 and reject_rate <= max_reject_rate
    return {
        "year_month": year_month,
        "taxi_type": taxi_type,
        "source_rows_footer": footer_rows,
        "read_rows": read_rows,
        "rejects": {r: counts.get(f"r_{r}") for r in REJECT_RULES},
        "flags": {f: counts.get(f"f_{f}") for f in FLAG_RULES},
        "written_rows": written_rows,
        "reject_rate": round(reject_rate, 6),
        "status": "PASS" if passed else "FAIL",
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def process_type(spark: SparkSession, bucket: str, taxi_type: str, year_month: str, footer_rows: int,
                 max_reject_rate: float, run_id: str) -> tuple[DataFrame, dict]:
    path = f"s3://{bucket}/{bronze_prefix(taxi_type, year_month)}"
    canonical = to_canonical(spark.read.parquet(path), taxi_type, path)
    rejects = reject_conditions(taxi_type, year_month)
    report = build_report(taxi_type, year_month, footer_rows,
                          count_rules(canonical, rejects, flag_conditions(taxi_type)), max_reject_rate, run_id)
    clean = canonical.where(~reduce(Column.__or__, rejects.values()))
    return clean, report


def main() -> None:
    args = parse_args(["BUCKET", "YEAR_MONTH", "TAXI_TYPES", "TABLE_FORMAT"],
                      {"MAX_REJECT_RATE": "0.10", "JOB_RUN_ID": "local"})
    bucket = args["BUCKET"]
    year_month = resolve_year_month(bucket, args["YEAR_MONTH"])
    spark = build_spark("nyc-taxi-bronze-to-silver", bucket)
    footer_rows = {f["taxi_type"]: f["num_rows"] for f in read_json(bucket, manifest_key(year_month))["files"]}

    results = [process_type(spark, bucket, t, year_month, footer_rows[t], float(args["MAX_REJECT_RATE"]),
                            args["JOB_RUN_ID"])
               for t in parse_taxi_types(args["TAXI_TYPES"])]
    for _, report in results:
        print(f"[DQ] {json.dumps(report)}")
        write_json(bucket, dq_report_key(year_month, report["taxi_type"]), report)
    failed = [r["taxi_type"] for _, r in results if r["status"] == "FAIL"]
    if failed:
        raise RuntimeError(f"DQ FAIL for {year_month}: {failed}; nothing written to Silver")

    silver = reduce(DataFrame.unionByName, [df for df, _ in results])
    write_month(spark, silver, "trips_clean", table_format=args["TABLE_FORMAT"], bucket=bucket,
                year_month=year_month, iceberg_partitions="months(pickup_datetime), taxi_type")


if __name__ == "__main__":
    main()
