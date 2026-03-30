from pyspark.sql.types import *
from pyspark.sql import SparkSession, functions as F

def create_spark_session():
    return (
        SparkSession.builder
        .appName("SilverWatchEvents")
        .master("local[*]")
        .getOrCreate()
    )

schema = StructType(
    [
        StructField("event_id",StringType()),
        StructField("event_type",StringType()),
        StructField("user_id",StringType()),
        StructField("session_id",StringType()),
        StructField("content_id",StringType()),
        StructField("event_time",StringType()),
        StructField("playback_position",IntegerType())
    ]
)

def read_bronze(spark):
    return (
        spark.readStream
        .format("parquet")
        .load("data/bronze/watch_events")
    )

def parse_json(df):
    return df.withColumn("json", F.from_json(F.col("raw_value"), schema))

def split_valid_invalid(df):
    invalid_condition = (
        F.col("json").isNull() | F.col("json.event_id").isNull() | F.col("json.user_id").isNull() | F.col("json.event_type").isNull()
    )

    valid_df = (
        df.filter(~invalid_condition)
        .select(
            "json.*",
            "event_key",
            "kafka_timestamp",
            "ingest_ts",
            "raw_value"
        )
    )

    invalid_df = (
        df.filter(invalid_condition)
        .select(
            "json.*",
            "event_key",
            "kafka_timestamp",
            "ingest_ts",
            "raw_value"
        )
    )

    return valid_df, invalid_df

def write_valid(df):
    return (
        df.writeStream
        .format("delta")
        .option("path","data/silver/watch_events")
        .option("checkpointLocation","checkpoints/silver/watch_events")
    )