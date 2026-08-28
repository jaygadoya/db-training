"""
will arrive every 40 seconds processingTime = 40s.
    The data will be loaded into table: data_lake.telecom
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

SOURCE_PATH  = "/workspace/landing/data_lake/telecom/data"
CHECKPOINT   = "/workspace/landing/data_lake/telecom/_checkpoint"
SCHEMA_LOC   = "/workspace/landing/data_lake/telecom/_schema"
TARGET_DB    = "data_lake"
TARGET_TABLE = "telecom"

SCHEMA_HINTS = (
    "telecom_id STRING, customer_id STRING, customer_name STRING, "
    "phone_number STRING, region STRING, city STRING, network_type STRING, "
    "tower_id STRING, signal_strength INT, data_usage_gb DOUBLE, "
    "call_minutes INT, sms_count INT, monthly_bill DOUBLE, "
    "plan_type STRING, subscription_year INT, device_type STRING, "
    "roaming_enabled STRING, status STRING, latency_ms DOUBLE, "
    "customer_satisfaction DOUBLE"
)


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("autoloader_telecom").getOrCreate()


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
        .trigger(processingTime="40 seconds")
        .toTable(f"{TARGET_DB}.{TARGET_TABLE}")
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()