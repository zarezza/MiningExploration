# Mining Exploration
Comperhensive data exploration for mining surveyors and operators.

## Setup

first, connect to your gcloud acount on your device terminal
```shell
gcloud auth login
```

Create a new project
```shell
gcloud projects create PROJECT_ID
gcloud config set project PROJECT_ID
```

Setup the project by running setup script
```shell
./setup.bash
```

Note: This step will take you a while, you might be exitted while running the script. If that happen, simply running the script again with the same name will work

## preprocess data

In this scenario, surveyor must push the data to the bucket and after that a job can be submitted. To run this process, simply running:
```shell
./send_data.bash <cluster-name> <bucket-name> <path/to/file.csv>
```
Sample file can be downloaded from here: https://ecat.ga.gov.au/geonetwork/srv/eng/catalog.search#/metadata/145120

After that is done, surveyor can then run this process to perform query to the table.
```shell
./update_preview.bash <cluster-name> <bucket-name>
```

Then surveyor can view the result by copying the notebook file (minex_view.ipynb) to Bigquery studio. Sample results can be viewed from the current notebook file.

## delete data (for dev)

This removes all table from metastore
```shell
gcloud dataproc jobs submit pyspark --cluster=minex-spark-cluster --region="us-central1" gs://<bucket_name>/jobs/drop_all.py
```