#!/bin/bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# cdamien@ 2026

set -e
source .venv/bin/activate
pip install -q --upgrade 'apache-beam[gcp]' pyopenssl

# Please customize these values before running.
export PROJECT_ID="YOUR_GCP_PROJECT_ID"
export BUCKET_NAME="YOUR_GCS_BUCKET_NAME"
export DATASET_ID="YOUR_BQ_DATASET_ID"
export TABLE_ID="YOUR_BQ_TABLE_ID"
export REGION="YOUR_REGION"

# Optional pattern overrides
# export FILE_PATTERN="*/*/*/deid_object_masked.json"
# export CUSTOM_FILE_PATTERN="gs://some-bucket/custom/path/file.json"

# Temporary bucket locations for Dataflow (assuming bucket is the same as the data here)
TEMP_LOCATION="${BUCKET_NAME}/temp"
STAGING_LOCATION="${BUCKET_NAME}/staging"

echo "--------------------------------------------------"
echo "Launching Dataflow Ingestion Pipeline..."
echo "Project: ${PROJECT_ID}"
if [ -n "${CUSTOM_FILE_PATTERN}" ]; then
  echo "Custom File Pattern: ${CUSTOM_FILE_PATTERN}"
else
  echo "Bucket: ${BUCKET_NAME}"
  if [ -n "${FILE_PATTERN}" ]; then
    echo "File Pattern: ${FILE_PATTERN}"
  else
    echo "File Pattern: [Default: */*/*/deid_object_masked.json]"
  fi
fi
echo "Dataset: ${DATASET_ID}"
echo "Table: ${TABLE_ID}"
echo "Region: ${REGION}"
echo "--------------------------------------------------"

CMD_ARGS=(
  --project "${PROJECT_ID}"
  --dataset "${DATASET_ID}"
  --table "${TABLE_ID}"
  --runner "DataflowRunner"
  --temp_location "${TEMP_LOCATION}"
  --staging_location "${STAGING_LOCATION}"
  --region "${REGION}"
  --job_name "gcs-to-bigquery-ingest-$(date +%Y%m%d-%H%M%S)"
)

if [ -n "${CUSTOM_FILE_PATTERN}" ]; then
  CMD_ARGS+=(--custom_file_pattern "${CUSTOM_FILE_PATTERN}")
else
  CMD_ARGS+=(--bucket "${BUCKET_NAME}")
  if [ -n "${FILE_PATTERN}" ]; then
    CMD_ARGS+=(--file_pattern "${FILE_PATTERN}")
  fi
fi

# Run the pipeline on Dataflow
python3 ingestion_pipeline.py "${CMD_ARGS[@]}"

echo "Pipeline submitted successfully."
