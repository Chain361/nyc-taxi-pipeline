#!/usr/bin/env bash
# Usage: BUCKET=... ATHENA_BUCKET=... LAB_ROLE_ARN=... ALERT_EMAIL=... infra/deploy.sh
# Validate first: aws cloudformation validate-template --template-body file://infra/template.yaml
set -euo pipefail
cd "$(dirname "$0")/.."
: "${BUCKET:?}" "${ATHENA_BUCKET:?}" "${LAB_ROLE_ARN:?}" "${ALERT_EMAIL:?}"
STACK="${STACK:-nyc-taxi-pipeline}"
ARTIFACTS="${BUCKET}-artifacts"   # holds the packaged state machine definition

aws s3api head-bucket --bucket "$ARTIFACTS" 2>/dev/null || aws s3 mb "s3://$ARTIFACTS" --region us-east-1
aws cloudformation package --template-file infra/template.yaml --s3-bucket "$ARTIFACTS" \
  --output-template-file infra/packaged.yaml
aws cloudformation deploy --stack-name "$STACK" --template-file infra/packaged.yaml --region us-east-1 \
  --parameter-overrides BucketName="$BUCKET" AthenaResultsBucketName="$ATHENA_BUCKET" \
    LabRoleArn="$LAB_ROLE_ARN" AlertEmail="$ALERT_EMAIL" ${MAX_CONCURRENCY:+MaxConcurrency=$MAX_CONCURRENCY} \
    ${TABLE_FORMAT:+TableFormat=$TABLE_FORMAT}
aws s3 sync src/ "s3://$BUCKET/scripts/" --exclude "*__pycache__*" --delete
aws cloudformation describe-stacks --stack-name "$STACK" --query "Stacks[0].Outputs" --output table
