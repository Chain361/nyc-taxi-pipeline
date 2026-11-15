"""Bronze ingest (Glue Python shell): TLC parquet per type for one month + zone reference files.

Exits non-zero on any failure. Re-runs skip a file only if the S3 copy has the same size as the source.
"""
from __future__ import annotations

from datetime import date

import boto3
import pyarrow.fs
import pyarrow.parquet as pq
import requests
from botocore.exceptions import ClientError

from job_utils import LATEST, LATEST_KEY, parse_args, parse_taxi_types, write_json
from taxi_types import bronze_prefix, manifest_key, tlc_file_name

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TIMEOUT_SECONDS = (10, 300)  # (connect, read)
LATEST_LOOKBACK_MONTHS = 12

s3 = boto3.client("s3")


def s3_size(bucket: str, key: str) -> int | None:
    try:
        return s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return None
        raise


def source_size(url: str) -> int | None:
    """Content-Length of the source, or None if unusable (missing, 0 from dynamic exports, or compressed)."""
    response = requests.head(url, timeout=TIMEOUT_SECONDS, allow_redirects=True)
    response.raise_for_status()
    if response.headers.get("Content-Encoding"):
        return None  # length is of the compressed body, not what lands in S3
    length = int(response.headers.get("Content-Length") or 0)
    return length or None


def upload(url: str, bucket: str, key: str) -> None:
    """Stream url -> S3, skipping only when a copy of the known source size exists; a truncated upload is deleted and raises."""
    expected = source_size(url)
    existing = s3_size(bucket, key)
    # Unknown source size -> always re-download (only small reference files); never trust a stale copy.
    if existing is not None and existing == expected:
        print(f"[SKIP] s3://{bucket}/{key} ({existing} bytes)")
        return
    with requests.get(url, stream=True, timeout=TIMEOUT_SECONDS) as response:
        response.raise_for_status()
        response.raw.decode_content = True  # store decoded bytes, not gzip
        s3.upload_fileobj(response.raw, bucket, key)
    actual = s3_size(bucket, key)
    if expected is not None and actual != expected:
        s3.delete_object(Bucket=bucket, Key=key)
        raise IOError(f"Truncated upload {key}: {actual} of {expected} bytes")
    print(f"[OK] {url} -> s3://{bucket}/{key} ({actual} bytes)")


def footer_num_rows(bucket: str, key: str) -> int:
    """Row count from the parquet footer (range read, no full download)."""
    fs = pyarrow.fs.S3FileSystem(region=s3.meta.region_name)
    with fs.open_input_file(f"{bucket}/{key}") as f:
        return pq.ParquetFile(f).metadata.num_rows


def month_back(year_month: str, months: int) -> str:
    year, month = map(int, year_month.split("-"))
    index = year * 12 + month - 1 - months
    return f"{index // 12:04d}-{index % 12 + 1:02d}"


def resolve_latest(taxi_types: list[str]) -> str:
    """Newest month whose file exists for every requested type (HEAD probe, newest first)."""
    this_month = date.today().strftime("%Y-%m")
    for back in range(LATEST_LOOKBACK_MONTHS):
        candidate = month_back(this_month, back)
        urls = [f"{TLC_BASE_URL}/{tlc_file_name(t, candidate)}" for t in taxi_types]
        if all(requests.head(u, timeout=TIMEOUT_SECONDS).ok for u in urls):
            return candidate
    raise RuntimeError(f"No month with all of {taxi_types} in the last {LATEST_LOOKBACK_MONTHS} months")


def ingest_trip_file(bucket: str, taxi_type: str, year_month: str) -> dict:
    file_name = tlc_file_name(taxi_type, year_month)
    url = f"{TLC_BASE_URL}/{file_name}"
    key = bronze_prefix(taxi_type, year_month) + file_name
    upload(url, bucket, key)
    return {"taxi_type": taxi_type, "source_url": url, "key": key,
            "bytes": s3_size(bucket, key), "num_rows": footer_num_rows(bucket, key)}


def main() -> None:
    args = parse_args(["BUCKET", "YEAR_MONTH", "TAXI_TYPES", "ZONE_LOOKUP_URL", "ZONES_GEOJSON_URL"])
    bucket = args["BUCKET"]
    taxi_types = parse_taxi_types(args["TAXI_TYPES"])
    year_month = resolve_latest(taxi_types) if args["YEAR_MONTH"] == LATEST else args["YEAR_MONTH"]

    upload(args["ZONE_LOOKUP_URL"], bucket, "bronze/lookup/taxi_zone_lookup.csv")
    upload(args["ZONES_GEOJSON_URL"], bucket, "bronze/geo/taxi_zones.geojson")

    files = [ingest_trip_file(bucket, t, year_month) for t in taxi_types]
    write_json(bucket, manifest_key(year_month), {"year_month": year_month, "files": files})
    if args["YEAR_MONTH"] == LATEST:
        write_json(bucket, LATEST_KEY, {"year_month": year_month})
    print(f"[DONE] {year_month}: {[(f['taxi_type'], f['num_rows']) for f in files]}")


if __name__ == "__main__":
    main()
