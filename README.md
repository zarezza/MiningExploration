
## preprocess data

upload raw file
```shell
gcloud storage cp /path/to/raw_file gs://<bucket_name>/raw
```

run spark
```shell
gcloud dataproc jobs submit pyspark --cluster=minex-spark-cluster --region="us-central1" gs://minex-1238/jobs/preprocess.py -- --bucket="minex-1238"

```

## CRUD operation
