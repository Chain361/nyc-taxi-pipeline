#!/usr/bin/env bash
# Usage: BUCKET=... ATHENA_BUCKET=... infra/teardown.sh   (deletes ALL data in both buckets)
set -euo pipefail
: "${BUCKET:?}" "${ATHENA_BUCKET:?}"
STACK="${STACK:-nyc-taxi-pipeline}"
for b in "$BUCKET" "$ATHENA_BUCKET" "${BUCKET}-artifacts"; do
  aws s3 rm "s3://$b" --recursive || true
done
# Iceberg tables are created by the jobs, not the stack: drop them so the database can be deleted.
for t in trips_clean fact_zone_month dim_zone dq_reports; do
  aws glue delete-table --database-name nyc_taxi --name "$t" 2>/dev/null || true
done
aws cloudformation delete-stack --stack-name "$STACK" --region us-east-1
aws cloudformation wait stack-delete-complete --stack-name "$STACK" --region us-east-1
aws s3 rb "s3://${BUCKET}-artifacts" || true
