import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from pyproj import Transformer

def convert_coordinates_udf():
    transformer = Transformer.from_crs("EPSG:28354", "EPSG:4326", always_xy=True)
    def transform_coords(easting, northing):
        try:
            lon, lat = transformer.transform(float(easting), float(northing))
            return (float(lon), float(lat))
        except:
            return (None, None)
    return F.udf(transform_coords, "struct<longitude:double,latitude:double>")

def sql_query(spark, sql_path: str, output_path: str):
    sql_query_text = None
    
    try:
        print(f"Attempting to read SQL file using Spark: {sql_path}")
        sql_content = spark.read.text(sql_path).collect()
        sql_query_text = "\n".join([row.value for row in sql_content])
        print("Read SQL Successful")
    except Exception as e:
        print(f"Error reading SQL file: {e}")
    
    if not sql_query_text:
        print("No SQL content retrieved")
        return None
    
    # Execute the query
    print(f"Executing SQL query from: {sql_path}")
    try:
        query_results = spark.sql(sql_query_text)
        query_results.show(n=5, truncate=False)  

        # Save results
        print(f"Saving Query results to: {output_path}")
        query_results.write.parquet(output_path, mode="overwrite")
        
        return query_results
    except Exception as e:
        print(f"Error executing SQL query: {e}")
        return None

def main(args):
    BUCKET_NAME = args.bucket_name
    print(f"Bucket name: {BUCKET_NAME}")

    INPUT_FILE_PATH = f"gs://{BUCKET_NAME}/raw/QLD_MGA54_20210825.csv"
    print(f"Input file path: {INPUT_FILE_PATH}")

    OUTPUT_SUMMARY_PATH = f"gs://{BUCKET_NAME}/processed/survey_summary.parquet"
    OUTPUT_DEPTH_ANOMALY_PATH = f"gs://{BUCKET_NAME}/processed/depth_anomaly.parquet"

    SQL_SURVEY_SUMMARY = f"gs://{BUCKET_NAME}/sql/survey_summary.sql"
    SQL_DEPTH_ANOMALY = f"gs://{BUCKET_NAME}/sql/depth_anomaly.sql"

    try:
        spark = SparkSession.builder \
            .appName("SurveyDataSparkETLJob") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        
        print(f"Reading CSV from: {INPUT_FILE_PATH}")
        df_raw = spark.read.csv(INPUT_FILE_PATH, header=True, inferSchema=True)
        print("CSV read successfully.")
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    print(f"Total raw record count: {df_raw.count()}")
    print(f"Column names: {df_raw.columns}")

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
    print(f"Record count (filter low confidence): {df_clean.count()}")

    # Filter negative depths
    df_clean = df_clean.filter(F.col("DEPTH") >= 0)
    print(f"Record count (filter negative depths): {df_clean.count()}")

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

    print(f"Total processed record after cleaning: {df_processed.count()}")
    
    df_processed.createOrReplaceTempView("survey_data_view")

    sql_query(spark, SQL_SURVEY_SUMMARY, OUTPUT_SUMMARY_PATH)
    sql_query(spark, SQL_DEPTH_ANOMALY, OUTPUT_DEPTH_ANOMALY_PATH)

    print("Spark queries completed.")
    
    spark.stop()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Process survey data")
    parser.add_argument("--bucket-name", type=str, required=True, help="Bucket name")
    args = parser.parse_args()

    main(args)
