from pyspark.sql import SparkSession, functions as F

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("bronze-watch-events")
        .master("local[*]")
        # additional configuration for local spark with Kafka connector
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                "io.delta:delta-spark_2.12:3.1.0"
            ])
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_watch_events_from_kafka(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers","localhost:9092")
        .option("subscribe","watch-events")
        .option("startingOffsets", "earliest")
        .load()
    )

def prepare_bronze_dataframe(kafka_df):
    return (
        kafka_df
        .select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("timestampType"),
            F.col("key").cast("string").alias("event_key"),
            F.col("value").cast("string").alias("raw_value"),
            F.current_timestamp().alias("ingest_ts")
        )
        .withColumn("date",F.to_date(F.col("ingest_ts"),"DD/MM/YYYY"))
    )

def write_bronze_stream(bronze_df):
    # Reducing number of output files in streaming (small files problem)
    # I force a fixed number of partitions (4):
    # - streaming micro-batches can create many small files
    # - data currently falls into a single data partition
    # This is local/dev optimization only - in production this should be handled via proper partitioning strategy and compaction (OPTIMIZE)
    bronze_df = bronze_df.repartition(4)
    return (
        bronze_df.writeStream
        .format("delta")
        .partitionBy("date")
        .outputMode("append")
        .option("path","data/bronze/watch_events")
        .option("checkpointLocation","checkpoints/bronze/bronze_watch_events")
        .trigger(processingTime="30 seconds")
        .start()
    )

def main() -> None:
    spark = create_spark_session()

    kafka_df = read_watch_events_from_kafka(spark)
    bronze_df = prepare_bronze_dataframe(kafka_df)

    query = write_bronze_stream(bronze_df)
    query.awaitTermination()

if __name__ == "__main__":
    main()





