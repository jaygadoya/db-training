"""
will arrive once in a day and shall trigger at a praticular time. This is a batch job which runs daily.
    The data will be loaded into table: raw_data.sat_comm

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

SOURCE_PATH  = "/workspace/landing/raw_data/sat_comm/data"
CHECKPOINT   = "/workspace/landing/raw_data/sat_comm/_checkpoint"
SCHEMA_LOC   = "/workspace/landing/raw_data/sat_comm/_schema"
TARGET_DB    = "raw_data"
TARGET_TABLE = "sat_comm"

SCHEMA_HINTS = (
    "comm_id STRING, satellite_name STRING, latitude DOUBLE, "
    "longitude DOUBLE, altitude_km DOUBLE, frequency_band STRING, "
    "uplink_mhz DOUBLE, downlink_mhz DOUBLE, status STRING, operator STRING"
)


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("autoloader_sat_comm").getOrCreate()


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
        .trigger(availableNow=True)
        .toTable(f"{TARGET_DB}.{TARGET_TABLE}")
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()