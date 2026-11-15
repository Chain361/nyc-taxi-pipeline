"""Local checks (no AWS, no Spark) for per-type mapping, month filter and DQ applicability."""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src" / "lib"))

from taxi_types import (COLUMN_MAP, FLAG_RULES, REJECT_RULES, TAXI_TYPES, month_bounds,  # noqa: E402
                        resolve_columns, rule_applies)


def test_yellow_airport_fee_matches_either_case():
    for name in ("Airport_fee", "airport_fee"):
        resolved = resolve_columns("yellow", ["tpep_pickup_datetime", "tpep_dropoff_datetime", name])
        assert resolved["airport_fee"] == name


def test_fhv_mixed_case_columns_resolve():
    file_columns = ["dispatching_base_num", "pickup_datetime", "dropOff_datetime", "PUlocationID", "DOlocationID",
                    "SR_Flag"]
    resolved = resolve_columns("fhv", file_columns)
    assert resolved["dropoff_datetime"] == "dropOff_datetime"
    assert resolved["pickup_location_id"] == "PUlocationID"
    assert resolved["trip_distance"] is None and resolved["fare_amount"] is None


def test_every_type_maps_required_columns_and_missing_ones_raise():
    for taxi_type in TAXI_TYPES:
        assert {"pickup_datetime", "dropoff_datetime", "pickup_location_id"} <= COLUMN_MAP[taxi_type].keys()
    with pytest.raises(ValueError):
        resolve_columns("green", ["VendorID"])


def test_month_filter_keeps_only_the_files_month():
    frame = pd.DataFrame({"pickup_datetime": pd.to_datetime(
        ["2024-01-31 23:59:59", "2024-02-01 00:00:00", "2024-02-29 23:59:59", "2024-03-01 00:00:00", "2002-02-10 08:00:00"])})
    start, end = month_bounds("2024-02")
    kept = frame[(frame["pickup_datetime"] >= start) & (frame["pickup_datetime"] < end)]
    assert len(kept) == 2
    assert month_bounds("2024-12")[1].isoformat() == "2025-01-01T00:00:00"


def test_dq_rules_apply_only_where_columns_exist():
    expected_not_applicable = {
        "yellow": set(),
        "green": set(),
        "fhv": {"negative_amount", "negative_distance", "passenger_count_invalid"},
        "hvfhv": {"passenger_count_invalid"},
    }
    for taxi_type, not_applicable in expected_not_applicable.items():
        actual = {r for r in REJECT_RULES + FLAG_RULES if not rule_applies(r, taxi_type)}
        assert actual == not_applicable, taxi_type
