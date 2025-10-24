import sys
import argparse
from pyspark.sql import SparkSession

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def printer(message: str, color: str = None):
    if color is None:
        print(message)
    else:
        print(f"{color}{message}{bcolors.ENDC}")
    
def sql_query(spark, sql_path: str, output_path: str):
    sql_query_text = None
    
    try:
        printer(f"Attempting to read SQL file using Spark: {sql_path}")
        sql_content = spark.read.text(sql_path).collect()
        sql_query_text = "\n".join([row.value for row in sql_content])
        printer("Read SQL Successful")
    except Exception as e:
        printer(f"Error reading SQL file: {e}", bcolors.FAIL)
    
    if not sql_query_text:
        printer("No SQL content retrieved", bcolors.FAIL)
        return None
    
    # Execute the query
    printer(f"Executing SQL query from: {sql_path}")
    try:
        query_results = spark.sql(sql_query_text)
        query_results.show(n=5, truncate=False)  

        # Save results
        printer(f"Saving Query results to: {output_path}", bcolors.OKGREEN)
        query_results.write.parquet(output_path, mode="overwrite")
        
        return query_results
    except Exception as e:
        printer(f"Error executing SQL query: {e}", bcolors.FAIL)
        return None

def main(args):
    BUCKET_NAME = args.bucket_name
    METASTORE_DATABASE = "survey"
    BASE_PROCESSED_TABLE = "base_processed"
    
    printer(f"Bucket name: {BUCKET_NAME}", bcolors.OKBLUE)
    printer(f"Source database: {METASTORE_DATABASE}", bcolors.OKBLUE)
    printer(f"Source table: {BASE_PROCESSED_TABLE}", bcolors.OKBLUE)

    queries = [
        (f"gs://{BUCKET_NAME}/sql/survey_summary.sql", f"gs://{BUCKET_NAME}/processed/survey_summary.parquet"),
        (f"gs://{BUCKET_NAME}/sql/depth_anomaly.sql", f"gs://{BUCKET_NAME}/processed/depth_anomaly.parquet"),
        (f"gs://{BUCKET_NAME}/sql/boundary_depth.sql", f"gs://{BUCKET_NAME}/processed/boundary_depth.parquet"),
        (f"gs://{BUCKET_NAME}/sql/regional_coverage.sql", f"gs://{BUCKET_NAME}/processed/regional_coverage.parquet"),
    ]

    try:
        spark = SparkSession.builder \
            .appName("RunQueryJob") \
            .enableHiveSupport() \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        printer("Spark session initialized successful", bcolors.OKGREEN)
    except Exception as e:
        printer(f"Error initializing Spark session: {e}", bcolors.FAIL)
        sys.exit(1)

    for sql_path, output_path in queries:
        printer(f"Executing query from: {sql_path}", bcolors.OKBLUE)
        sql_query(spark, sql_path, output_path)

    printer("Spark process completed", bcolors.OKGREEN)
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run SQL queries against base processed table")
    parser.add_argument("--bucket-name", type=str, required=True, help="Bucket name")
    args = parser.parse_args()

    main(args)