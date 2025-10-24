#!/bin/bash
set -e  # Exit on error

# Check arguments
if [ $# -ne 3 ]; then
    echo "Usage: $0 <cluster_name> <bucket_name> <local_file_path>"
    echo "Example: $0 my-cluster my-data-bucket /path/to/data.csv"
    exit 1
fi

CLUSTER_NAME=$1
BUCKET_NAME=$2
LOCAL_FILE=$3

# Validate local file exists
if [ ! -f "$LOCAL_FILE" ]; then
    echo "Error: File '$LOCAL_FILE' does not exist"
    exit 1
fi

# Generate timestamp and filename
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
FILE_EXTENSION="${LOCAL_FILE##*.}"
BASE_NAME=$(basename "$LOCAL_FILE" ".$FILE_EXTENSION")
REMOTE_FILENAME="${BASE_NAME}_${TIMESTAMP}.${FILE_EXTENSION}"

echo "Uploading file to gs://${BUCKET_NAME}/raw/${REMOTE_FILENAME}..."

# Upload file to GCS
gcloud storage cp "$LOCAL_FILE" "gs://${BUCKET_NAME}/raw/${REMOTE_FILENAME}"

echo "Upload complete. Starting Dataproc preprocess job..."

# Submit Dataproc job
gcloud dataproc jobs submit pyspark \
    --cluster=$CLUSTER_NAME \
    --region=us-central1 \
    "gs://${BUCKET_NAME}/jobs/preprocess.py" \
    -- \
    --bucket-name="$BUCKET_NAME" \
    --data="gs://${BUCKET_NAME}/raw/${REMOTE_FILENAME}"

echo "Job completed."