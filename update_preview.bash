#!/bin/bash
set -e  # Exit on error

# Check arguments
if [ $# -ne 2 ]; then
    echo "Usage: $0 <cluster_name> <bucket_name>"
    echo "Example: $0 my-cluster my-data-bucket"
    exit 1
fi

CLUSTER_NAME=$1
BUCKET_NAME=$2

gcloud dataproc jobs submit pyspark \
    --cluster=$CLUSTER_NAME \
    --region=us-central1 \
    "gs://${BUCKET_NAME}/jobs/run_query.py" \
    -- \
    --bucket-name="$BUCKET_NAME"

echo "All jobs completed successfully."