"""Per-type source mappings and DQ rule applicability, expressed as data.

Pure Python (no Spark/AWS) so it can be unit-tested locally and shared by
ingest, bronze_to_silver and the tests.
"""
from __future__ import annotations

from datetime import datetime

TAXI_TYPES = ("yellow", "green", "fhv", "hvfhv")
TAXI_ONLY = ("yellow", "green")

# TLC publishes HVFHV files under the "fhvhv" prefix.
TLC_FILE_PREFIX = {"yellow": "yellow", "green": "green", "fhv": "fhv", "hvfhv": "fhvhv"}

# Canonical Silver columns (besides taxi_type, is_shared and lineage) and their Spark types.
CANONICAL_COLUMNS = {
    "operator": "string",
    "pickup_datetime": "timestamp",
    "dropoff_datetime": "timestamp",
    "pickup_location_id": "int",
    "dropoff_location_id": "int",
    "passenger_count": "int",
    "trip_distance": "double",
    "fare_amount": "double",
    "tip_amount": "double",
    "total_amount": "double",
    "base_passenger_fare": "double",
    "tolls": "double",
    "bcf": "double",
    "sales_tax": "double",
    "tips": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
    "cbd_congestion_fee": "double",
}

_TAXI_FARES = ("fare_amount", "tip_amount", "total_amount", "congestion_surcharge", "cbd_congestion_fee")
_HVFHV_FARES = ("base_passenger_fare", "tolls", "bcf", "sales_tax", "congestion_surcharge",
                "airport_fee", "tips", "cbd_congestion_fee")


def _same_name(columns: tuple[str, ...]) -> dict[str, str]:
    return {c: c for c in columns}


# canonical column -> source column name (matched case-insensitively). Unmapped canonical columns are null.
COLUMN_MAP: dict[str, dict[str, str]] = {
    "yellow": {
        "operator": "VendorID",
        "pickup_datetime": "tpep_pickup_datetime",
        "dropoff_datetime": "tpep_dropoff_datetime",
        "pickup_location_id": "PULocationID",
        "dropoff_location_id": "DOLocationID",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        "airport_fee": "Airport_fee",
        **_same_name(_TAXI_FARES),
    },
    "green": {
        "operator": "VendorID",
        "pickup_datetime": "lpep_pickup_datetime",
        "dropoff_datetime": "lpep_dropoff_datetime",
        "pickup_location_id": "PULocationID",
        "dropoff_location_id": "DOLocationID",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        **_same_name(_TAXI_FARES),
    },
    "fhv": {
        "operator": "dispatching_base_num",
        "pickup_datetime": "pickup_datetime",
        "dropoff_datetime": "dropOff_datetime",
        "pickup_location_id": "PUlocationID",
        "dropoff_location_id": "DOlocationID",
    },
    "hvfhv": {
        "operator": "hvfhs_license_num",
        "pickup_datetime": "pickup_datetime",
        "dropoff_datetime": "dropoff_datetime",
        "pickup_location_id": "PULocationID",
        "dropoff_location_id": "DOLocationID",
        "trip_distance": "trip_miles",
        **_same_name(_HVFHV_FARES),
    },
}

REQUIRED_COLUMNS = ("pickup_datetime", "dropoff_datetime")

# Shared/pooled trip definition: (source column, values meaning "shared"). Types not listed are never shared.
SHARED_FLAG = {
    "hvfhv": ("shared_match_flag", ("Y",)),
    "fhv": ("SR_Flag", ("1", "1.0")),
}

# Amount columns checked by the negative_amount rule; types not listed have no fare (rule not applicable).
NEGATIVE_AMOUNT_COLUMNS = {
    "yellow": ("fare_amount", "total_amount"),
    "green": ("fare_amount", "total_amount"),
    "hvfhv": ("base_passenger_fare",),
}

REJECT_RULES = ("out_of_month", "dropoff_before_pickup", "negative_amount", "negative_distance")
FLAG_RULES = ("passenger_count_invalid", "pickup_location_null", "shared")


def resolve_columns(taxi_type: str, file_columns: list[str]) -> dict[str, str | None]:
    """Map every canonical column to the actual column name in the file (case-insensitive), or None."""
    by_lower = {c.lower(): c for c in file_columns}
    mapping = COLUMN_MAP[taxi_type]
    resolved = {canon: by_lower.get(mapping[canon].lower()) if canon in mapping else None
                for canon in CANONICAL_COLUMNS}
    missing = [c for c in REQUIRED_COLUMNS if resolved[c] is None]
    if missing:
        raise ValueError(f"{taxi_type}: required columns {missing} not found in {file_columns}")
    return resolved


def rule_applies(rule: str, taxi_type: str) -> bool:
    """Whether a DQ rule is meaningful for a type; non-applicable rules are reported as null."""
    applicability = {
        "negative_amount": taxi_type in NEGATIVE_AMOUNT_COLUMNS,
        "negative_distance": "trip_distance" in COLUMN_MAP[taxi_type],
        "passenger_count_invalid": "passenger_count" in COLUMN_MAP[taxi_type],
    }
    return applicability.get(rule, True)


def month_bounds(year_month: str) -> tuple[datetime, datetime]:
    """[start, end) of a 'YYYY-MM' month."""
    start = datetime.strptime(year_month, "%Y-%m")
    end = start.replace(year=start.year + 1, month=1) if start.month == 12 else start.replace(month=start.month + 1)
    return start, end


def tlc_file_name(taxi_type: str, year_month: str) -> str:
    return f"{TLC_FILE_PREFIX[taxi_type]}_tripdata_{year_month}.parquet"


def bronze_prefix(taxi_type: str, year_month: str) -> str:
    year, month = year_month.split("-")
    return f"bronze/taxi_type={taxi_type}/year={year}/month={month}/"


def manifest_key(year_month: str) -> str:
    return f"manifest/year_month={year_month}/manifest.json"


def dq_report_key(year_month: str, taxi_type: str) -> str:
    return f"dq_reports/year_month={year_month}/taxi_type={taxi_type}/report.json"
