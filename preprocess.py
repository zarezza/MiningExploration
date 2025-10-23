import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from pyproj import Transformer

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

def convert_coordinates_udf():
    transformer = Transformer.from_crs("EPSG:28354", "EPSG:4326", always_xy=True)
    def transform_coords(easting, northing):
        try:
            lon, lat = transformer.transform(float(easting), float(northing))
            return (float(lon), float(lat))
        except:
            return (None, None)
    return F.udf(transform_coords, "struct<longitude:double,latitude:double>")

def main(args):
    BUCKET_NAME = args.bucket_name
    METASTORE_DATABASE = "survey"
    BASE_PROCESSED_TABLE = "base_processed"

    INPUT_FILE_PATH = f"gs://{BUCKET_NAME}/raw/QLD_MGA54_20210825.csv"
    printer(f"Input file path: {INPUT_FILE_PATH}", bcolors.OKBLUE)

    try:
        spark = SparkSession.builder \
            .appName("SurveyDataSparkETLJob") \
            .enableHiveSupport() \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        
        printer(f"Reading CSV from: {INPUT_FILE_PATH}")
        df_raw = spark.read.csv(INPUT_FILE_PATH, header=True, inferSchema=True)
        printer("Read CSV successful.", bcolors.OKBLUE)
    except Exception as e:
        printer(f"Error reading CSV: {e}", bcolors.FAIL)
        sys.exit(1)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {METASTORE_DATABASE}")

    printer(f"Total raw record count: {df_raw.count()}")
    printer(f"Column names: {df_raw.columns}")

    # Selected relevant columns
    cols = [  
        "SURVEY_NAME", "SURVEY_LINE", "VERTEX", "SEGMENT_ID",
        "EASTING", "NORTHING", "LATITUDE", "LONGITUDE",
        "ELEVATION", "AEM_DEM", "DEPTH",
        "DEPTH_CONFIDENCE",
        "BOUNDARY_NAME", "AGE_TYPE"
    ]

    df_clean = df_raw.select([c for c in cols if c in df_raw.columns])

    # Set numeric columns
    numeric_cols = ["EASTING", "NORTHING", "ELEVATION", "AEM_DEM", "DEPTH"]
    
    # Remove nulls in numeric columns
    df_clean = df_clean.dropna(subset=numeric_cols)
    printer(f"Filter nulls | Record count: {df_clean.count()}", bcolors.OKBLUE)

    # Cast double types
    for col in numeric_cols:
        df_clean = df_clean.withColumn(col, F.col(col).cast("double"))

    # Convert categorical confidence to numeric scale (H=1.0, M=0.5, L=0.2)
    df_clean = df_clean.withColumn(
        "DEPTH_CONF_WEIGHT",
        F.when(F.col("DEPTH_CONFIDENCE") == "H", 1.0)
         .when(F.col("DEPTH_CONFIDENCE") == "M", 0.5)
         .when(F.col("DEPTH_CONFIDENCE") == "L", 0.2)
         .otherwise(0) # non-matching values, ignored at filtering step
    )

    # Filter low confidence records
    df_clean = df_clean.filter(F.col("DEPTH_CONF_WEIGHT") >= 0.5)
    printer(f"Filter low confidence | Record count: {df_clean.count()}", bcolors.OKBLUE)

    # Filter negative depths
    df_clean = df_clean.filter(F.col("DEPTH") >= 0)
    printer(f"Filter negative depths | Record count: {df_clean.count()}", bcolors.OKBLUE)

    # Add coordinate transformation and derived grid columns
    coord_transform = convert_coordinates_udf()
    df_processed = (
        df_clean
        .withColumn("coords", coord_transform(F.col("EASTING"), F.col("NORTHING")))
        .withColumn("LONGITUDE", F.col("coords.longitude"))
        .withColumn("LATITUDE", F.col("coords.latitude"))
        .drop("coords")
        .withColumn("grid_x", (F.col("EASTING") / 100).cast("int"))
        .withColumn("grid_y", (F.col("NORTHING") / 100).cast("int"))
    )

    printer(f"Total processed record after cleaning: {df_processed.count()}", bcolors.OKBLUE)

    df_processed.write\
        .mode("overwrite")\
        .format("parquet")\
        .saveAsTable(f"{BASE_PROCESSED_TABLE}")

    printer(f"Processed data saved to table: {BASE_PROCESSED_TABLE}", bcolors.OKBLUE)
    printer("Verifying saved table data...", bcolors.OKBLUE)

    df_verify = spark.table(f"{BASE_PROCESSED_TABLE}")
    df_verify.show(5)

    printer("Spark process completed.", bcolors.OKGREEN)
    spark.stop()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Process survey data")
    parser.add_argument("--bucket-name", type=str, required=True, help="Bucket name")
    args = parser.parse_args()

    main(args)
