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

def main():
    METASTORE_DATABASE = "survey"
    BASE_PROCESSED_TABLE = "base_processed"
    
    printer(f"Target database: {METASTORE_DATABASE}", bcolors.OKBLUE)
    printer(f"Target table: {BASE_PROCESSED_TABLE}", bcolors.OKBLUE)

    try:
        spark = SparkSession.builder \
            .appName("DropAllDataJob") \
            .enableHiveSupport() \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        df_view = spark.table(f"{METASTORE_DATABASE}.{BASE_PROCESSED_TABLE}")
        printer(f"Dataframe preview:\n{df_view.show(5)}")
        printer("Spark session initialized successful", bcolors.OKGREEN)
    except Exception as e:
        printer(f"Error initializing Spark session: {e}", bcolors.FAIL)
        sys.exit(1)

    spark.sql(f"TRUNCATE TABLE {METASTORE_DATABASE}.{BASE_PROCESSED_TABLE}")
    printer("Table truncated successful", bcolors.OKGREEN)

    # Verify deletion
    record_count_after = spark.table(f"{METASTORE_DATABASE}.{BASE_PROCESSED_TABLE}").count()
    printer(f"Records after deletion: {record_count_after:,}", bcolors.OKBLUE)

    printer("Spark process completed.", bcolors.OKGREEN)
    spark.stop()

if __name__ == "__main__":
    main()