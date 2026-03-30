from pyspark.sql.types import *
from pyspark.sql import SparkSession, functions as F

def create_spark_session():
    return (
        SparkSession.builder
        .appName("SilverWatchEvents")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                "io.delta:delta-spark_2.12:3.1.0"
            ])
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

bronze_schema = StructType([
    StructField("topic", StringType(), True),
    StructField("partition", IntegerType(), True),
    StructField("offset", LongType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
    StructField("timestampType", IntegerType(), True),
    StructField("event_key", StringType(), True),
    StructField("raw_value", StringType(), True),
    StructField("ingest_ts", TimestampType(), True),
    StructField("date", DateType(), True),
])

event_schema = StructType(
    [
        StructField("event_id",StringType()),
        StructField("event_type",StringType()),
        StructField("user_id",StringType()),
        StructField("session_id",StringType()),
        StructField("content_id",StringType()),
        StructField("event_time",StringType()),
        StructField("event_version",IntegerType()),
        StructField("event_header_reemission",IntegerType()),
        StructField("playback_position",IntegerType()),
        StructField("device_type",StringType()),
        StructField("platform",StringType()),
        StructField("network_type",StringType()),
        StructField("pause_reason",StringType()),
        StructField("resume_reason",StringType()),
        StructField("completion_percent",IntegerType()),
        StructField("watch_duration_sec",IntegerType()),
        StructField("abandoned_reason",StringType())
    ]
)

def read_bronze(spark):
    return (
        spark.readStream
        .format("parquet")
        .schema(bronze_schema)
        .load("data/bronze/watch_events")
    )

def parse_json(df):
    return df.withColumn("json", F.from_json(F.col("raw_value"), event_schema))

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
        .outputMode("append")
        .start()
    )

def write_invalid(df):
    return (
        df.writeStream
        .format("delta")
        .option("path", "data/silver/watch_events_invalid")
        .option("checkpointLocation", "checkpoints/silver/watch_events_invalid")
        .outputMode("append")
        .start()
    )

def main():
    spark = create_spark_session()
    bronze_df = read_bronze(spark)
    parsed_df = parse_json(bronze_df)
    valid_df,invalid_df = split_valid_invalid(parsed_df)
    query_valid = write_valid(valid_df)
    query_invalid = write_invalid(invalid_df)
    query_valid.awaitTermination()
    query_invalid.awaitTermination()

if __name__ == "__main__":
    main()