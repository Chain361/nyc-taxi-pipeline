"""Run the sql/ queries on Athena and save results to samples/ (plus the zones GeoJSON) for the README and app.

Usage: python scripts/export_samples.py --bucket <data-bucket> --athena-results-bucket <results-bucket>
"""
from __future__ import annotations

import argparse
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

ROOT = Path(__file__).resolve().parent.parent
SQL_DIR, SAMPLES_DIR = ROOT / "sql", ROOT / "samples"

# output csv -> sql file (or inline query)
EXPORTS = {
    "fact_zone_month.csv": "SELECT * FROM nyc_taxi.fact_zone_month ORDER BY year_month, taxi_type, pickup_location_id",
    "silver_sample_100.csv": "q_silver_sample.sql",
    "dq_summary.csv": "q_dq_summary.sql",
    "answer_original_h1_2024.csv": "q_original_h1_2024.sql",
    "answer_h1.csv": "q_h1.sql",
    "answer_h2.csv": "q_h2.sql",
    "answer_h3.csv": "q_h3.sql",
    "answer_h4.csv": "q_h4.sql",
    "answer_h5.csv": "q_h5.sql",
}


def load_sql(name_or_query: str, bucket: str) -> str:
    text = (SQL_DIR / name_or_query).read_text() if name_or_query.endswith(".sql") else name_or_query
    return text.replace("<BUCKET>", bucket).strip().rstrip(";")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--athena-results-bucket", required=True)
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()

    # Same credentials as the AWS CLI (~/.aws/credentials). Fail fast with a readable message.
    try:
        boto3.client("sts", region_name=args.region).get_caller_identity()
    except (ClientError, NoCredentialsError) as e:
        raise SystemExit(f"AWS credentials not usable ({e}).\n"
                         "Copy fresh ones from Learner Lab > AWS Details > AWS CLI into ~/.aws/credentials, "
                         "then check with: aws sts get-caller-identity")

    cursor = connect(s3_staging_dir=f"s3://{args.athena_results_bucket}/export/", region_name=args.region,
                     cursor_class=PandasCursor).cursor()
    cursor.execute(load_sql("ddl.sql", args.bucket))

    SAMPLES_DIR.mkdir(exist_ok=True)
    for csv_name, source in EXPORTS.items():
        df = cursor.execute(load_sql(source, args.bucket)).as_pandas()
        df.to_csv(SAMPLES_DIR / csv_name, index=False)
        print(f"{csv_name}: {len(df)} rows")

    boto3.client("s3", region_name=args.region).download_file(
        args.bucket, "gold/geo/taxi_zones.geojson", str(SAMPLES_DIR / "taxi_zones.geojson"))


if __name__ == "__main__":
    main()
