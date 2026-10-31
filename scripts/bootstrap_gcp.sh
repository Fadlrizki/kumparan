#!/usr/bin/env bash
# Bucket staging
set -euo pipefail
: "${PROJECT_ID:?export PROJECT_ID dulu}"
BUCKET="gs://${PROJECT_ID}-kumparan-staging"
LC=$(mktemp)
echo '{"rule":[{"action":{"type":"Delete"},"condition":{"age":7}}]}' > "$LC"   # parquet staging dibuang setelah 7 hari
gcloud storage buckets create "$BUCKET" --project "$PROJECT_ID" --location asia-southeast2 \
  --uniform-bucket-level-access --lifecycle-file="$LC" || echo "bucket sudah ada"
