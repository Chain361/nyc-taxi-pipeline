"""Argument parsing and small S3 JSON helpers shared by all jobs (no Spark dependency)."""
from __future__ import annotations

import argparse
import json

import boto3

LATEST = "latest"
LATEST_KEY = "manifest/latest.json"


def parse_args(required: list[str], optional: dict[str, str] | None = None) -> dict[str, str]:
    """Parse Glue-style `--NAME value` args; unknown Glue system args are ignored."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    for name in required:
        parser.add_argument(f"--{name}", required=True)
    for name, default in (optional or {}).items():
        parser.add_argument(f"--{name}", default=default)
    known, _ = parser.parse_known_args()
    return vars(known)


def parse_taxi_types(value: str) -> list[str]:
    """Accept a JSON list (from Step Functions States.JsonToString) or a comma-separated string."""
    return json.loads(value) if value.lstrip().startswith("[") else [t.strip() for t in value.split(",") if t.strip()]


def read_json(bucket: str, key: str) -> dict:
    body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body)


def write_json(bucket: str, key: str, payload: dict) -> None:
    # Single line: Athena's JsonSerDe (nyc_taxi.dq_reports) reads one JSON object per line.
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=json.dumps(payload).encode(),
                                  ContentType="application/json")


def resolve_year_month(bucket: str, year_month: str) -> str:
    """'latest' -> the month the ingest job resolved and recorded in manifest/latest.json."""
    return read_json(bucket, LATEST_KEY)["year_month"] if year_month == LATEST else year_month
