from datetime import timedelta

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from app.config.event_types import PLAY, PAUSE, RESUME, COMPLETED, ABANDONED

SOURCE_PATH = "data/silver/watch_events"
TARGET_PATH = "data/silver/watch_events_accumulated"
CHECKPOINT_PATH = "checkpoints/silver/watch_events_accumulated"
STATE_COLUMNS = [
        "device_type",
        "platform",
        "network_type",
        "completion_percent",
        "watch_duration_sec",
        "playback_position"
    ]

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SilverWatchEventsAccumulated")
        .master("local[2]")
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

def read_silver_parsed_stream(spark:SparkSession):
    return spark.readStream.format("delta").load(SOURCE_PATH)

#1 Step - DEDUPLICATION + latest wins
def deduplicate_batch(df):
    window = Window.partitionBy("event_id").orderBy(
            F.desc_nulls_last("event_time"),
            F.desc_nulls_last("event_version"),
            F.desc_nulls_last("event_header_reemission")
    )
    return (
        df
        .withColumn("row_num", F.row_number().over(window))
        .filter("row_num = 1")
        .drop("row_num")
        .withColumn("event_date", F.to_date("event_time"))
    )

#2 Step = Impacted Sessions
def get_impacted_sessions(df):
    return (
        df.select("session_id")
        .where("session_id is not null")
        .distinct()
    )

#3 Step - Date Range for batch
def get_batch_date_range(df, lookback_days=1):
    row = (
        df.select(
            F.min("event_date").alias("min_date"),
            F.max("event_date").alias("max_date")
        ).collect()[0]
    )

    if row["min_date"] is None:
        return None, None

    return (
        row["min_date"] - timedelta(days=lookback_days),
        row["max_date"] + timedelta(days=lookback_days)
    )

#4 Step - Lookback from Target
def read_target_lookback(spark,sessions_df,min_date,max_date):

    if not DeltaTable.isDeltaTable(spark, TARGET_PATH):
        return None

    target = (
        spark.read.format("delta")
        .load(TARGET_PATH)
        .where(
            F.col("event_date")>=F.lit(min_date) &
            F.col("event_date")<=F.lit(max_date)
        )
    )

    return (
        target.alias("t")
        .join(sessions_df.alias("s"), "session_id","inner")
    )

#5 Step - Accumulate State
def accumulate_state(df):
    window = (
        Window.partitionBy("session_id")
        .orderBy(
            F.asc_nulls_last("event_time"),
            F.asc_nulls_last("event_version"),
            F.asc_nulls_last("event_header_reemission"),
            F.asc_nulls_last("ingest_ts")
        )
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    #state derived from event_type
    state_event=(
        F.when(F.col("event_type")== PLAY, F.lit("PLAYING"))
        .when(F.col("event_type")== RESUME, F.lit("PLAYING"))
        .when(F.col("event_type") == PAUSE, F.lit("PAUSED"))
        .when(F.col("event_type") == ABANDONED, F.lit("ABANDONED"))
        .when(F.col("event_type") == COMPLETED, F.lit("COMPLETED"))
    )

    result_df = df.withColumn(
        "session_state",
        F.last(state_event, ignorenulls=True).over(window)
    )

    for col_name in STATE_COLUMNS:
        result_df = result_df.withColumn(
            f"current_{col_name}",
            F.last(F.col(col_name), True).over(window)
        )

    #building one standarized control event
    control_event=F.when(
        F.col("event_type").isin(PAUSE, RESUME, ABANDONED), F.struct(
            F.col("event_time").alias("event_time"),
            F.col("event_type").alias("event_type"),
            F.when(F.col("event_type")== PAUSE, F.col("pause_reason"))
            .when(F.col("event_type")== RESUME, F.col("resume_reason"))
            .when(F.col("event_type") == ABANDONED, F.col("abandoned_reason")).alias("reason")
        )
    )

    result_df = result_df.withColumn("control_event",control_event)

    #collecting control event history over session time
    result_df = result_df.withColumn(
        "control_event_history",
        F.collect_list("control_event").over(window)
    )
    result_df = result_df.drop("control_event")

    return result_df

#Step 6 - Filter Upsert Rows
def filter_upserts(accumulated_df,batch_df):
    event_ids = batch_df.select("event_id").distinct()
    return accumulated_df.join(event_ids, "event_id", "inner")

#Step 7 - Merge Into Delta
def merge_into_target(spark,df):
    if not DeltaTable.isDeltaTable(spark, TARGET_PATH):
        (
            df.write.format("delta")
            .partitionBy("event_date")
            .mode("overwrite")
            .save(TARGET_PATH)
        )
        return

    target = DeltaTable.forPath(spark, TARGET_PATH)

    (
        target.alias("t")
        .merge(
            df.alias("s"), "t.event_id=s.event_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

#Step 8 - Foreach Batch Pipeline
def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    spark = batch_df.sparkSession

    #1. dedup
    batch_df = deduplicate_batch(batch_df)

    #2. sessions
    sessions = get_impacted_sessions(batch_df)

    #3. date pruning
    min_date, max_date = get_batch_date_range(batch_df)

    #4. lookback
    lookback = read_target_lookback(spark,sessions,min_date,max_date)

    #5. combine
    if lookback is not None:
        working = lookback.unionByName(batch_df,allowMissingColumns=True)
    else:
        working = batch_df

    #6. accumulate
    accumulated = accumulate_state(working)

    #7. filter only current batch rows
    upserts = filter_upserts(accumulated,batch_df)

    #8. Merge
    merge_into_target(spark,upserts)

#main
def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    stream = read_silver_parsed_stream(spark)

    (
        stream.writeStream
        .foreachBatch(process_batch)
        .trigger(once=True)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
        .awaitTermination()
    )

if __name__ == "__main__":
    main()