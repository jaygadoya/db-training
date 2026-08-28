"""
will arrive every 2 minutes (micro-batch). As soon as the file arrives the autoloader should pick up the files from the location.
   The data will be loaded into table: bronze_layer.sat_nav
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

SOURCE_PATH  = "/workspace/landing/bronze_layer/sat_nav/data"
CHECKPOINT   = "/workspace/landing/bronze_layer/sat_nav/_checkpoint"
SCHEMA_LOC   = "/workspace/landing/bronze_layer/sat_nav/_schema"
TARGET_DB    = "bronze_layer"
TARGET_TABLE = "sat_nav"

SCHEMA_HINTS = (
    "sat_id STRING, latitude DOUBLE, longitude DOUBLE, "
    "altitude_km DOUBLE, status STRING"
)


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("autoloader_sat_nav").getOrCreate()


def main() -> None:
    spark = get_spark()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {TARGET_DB}")

    raw_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", SCHEMA_LOC)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", SCHEMA_HINTS)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .load(SOURCE_PATH)
        .withColumn("_ingest_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

    query = (
        raw_df.writeStream.format("delta")
        .option("checkpointLocation", CHECKPOINT)
        .outputMode("append")
        .trigger(processingTime="2 minutes")
        .toTable(f"{TARGET_DB}.{TARGET_TABLE}")
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()