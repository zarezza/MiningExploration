#!/bin/bash

set -e  # Exit on error

# Get user inputs
read -p "Enter GCS bucket name: " BUCKET_NAME
read -p "Enter Dataproc Metastore name: " METASTORE_NAME
read -p "Enter Dataproc cluster name: " CLUSTER_NAME
read -p "Enter region (default: us-central1): " REGION
REGION=${REGION:-us-central1}
PROJECT_ID=$(gcloud config get-value project)

echo ""
echo "Project ID: $PROJECT_ID"
echo "Bucket: $BUCKET_NAME"
echo "Metastore: $METASTORE_NAME"
echo "Cluster: $CLUSTER_NAME"
echo "Region: $REGION"
echo ""
read -p "Proceed with setup? (y/n): " CONFIRM

if [ "$CONFIRM" != "y" ]; then
    echo "Setup cancelled."
    exit 0
fi

echo "Creating Bucket..."
if gcloud storage buckets describe "gs://${BUCKET_NAME}" &>/dev/null; then
    echo "Bucket gs://${BUCKET_NAME} already exists, skipping..."
else
    gcloud storage buckets create "gs://${BUCKET_NAME}" \
        --location="$REGION" \
        --uniform-bucket-level-access
    
    # Create subdirectories
    echo "Creating bucket structure..."
    touch temp_file
    gcloud storage cp temp_file "gs://${BUCKET_NAME}/raw/.keep"
    gcloud storage cp temp_file "gs://${BUCKET_NAME}/processed/.keep"
    gcloud storage cp --recursive "./jobs/" "gs://${BUCKET_NAME}/jobs/"
    gcloud storage cp --recursive "./sql/" "gs://${BUCKET_NAME}/sql/"
    rm temp_file

    echo "Bucket ready"
fi

echo "Creating Dataproc Metastore..."
if gcloud metastore services describe "$METASTORE_NAME" --location="$REGION" &>/dev/null; then
    echo "Metastore $METASTORE_NAME already exists, skipping..."
else
    echo "Metastore creation initiated, this may take 15-20 minutes..."
    echo "If on this step the script exits due to timeout, please re-run the script."
    gcloud metastore services create "$METASTORE_NAME" \
        --location="$REGION" \
        --tier=DEVELOPER \
        --hive-metastore-version=3.1.2

    echo "Metastore ready"
fi

# Get metastore service URI
METASTORE_URI=$(gcloud metastore services describe "$METASTORE_NAME" \
    --location="$REGION" \
    --format="value(endpointUri)")

echo "Creating Dataproc Cluster..."
if gcloud dataproc clusters describe "$CLUSTER_NAME" --region="$REGION" &>/dev/null; then
    echo "Cluster $CLUSTER_NAME already exists, skipping..."
else
    gcloud dataproc clusters create "$CLUSTER_NAME" \
        --region="$REGION" \
        --zone="${REGION}-a" \
        --master-machine-type=e2-standard-2 \
        --master-boot-disk-size=100 \
        --num-workers=2 \
        --worker-machine-type=e2-standard-2 \
        --worker-boot-disk-size=100 \
        --image-version=2.1-debian11 \
        --optional-components=JUPYTER \
        --enable-component-gateway \
        --dataproc-metastore="projects/${PROJECT_ID}/locations/${REGION}/services/${METASTORE_NAME}" \
        --properties="spark:spark.executor.memory=3g,spark:spark.executor.cores=2" \
        --metadata="gcs-connector-version=2.2.11"

    echo "Cluster ready"
fi

echo "Setup complete."