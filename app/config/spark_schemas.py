from pyspark.sql.types import StructType,StructField,StringType,IntegerType

WATCH_EVENT_TYPES = {"play","pause","resume","completed","abandoned"}

WATCH_EVENT_SPARK_SCHEMA = StructType(
    [
        StructField("event_id",StringType(),True),
        StructField("event_type",StringType(),True),
        StructField("user_id",StringType(),True),
        StructField("session_id",StringType(),True),
        StructField("content_id",StringType(),True),
        StructField("event_time",StringType(),True),
        StructField("event_version",IntegerType(),True),
        StructField("event_header_reemission",IntegerType(),True),
        StructField("playback_position",IntegerType(),True),
        StructField("device_type",StringType(),True),
        StructField("platform",StringType(),True),
        StructField("network_type",StringType(),True),
        StructField("pause_reason",StringType(),True),
        StructField("resume_reason",StringType(),True),
        StructField("completion_percent",IntegerType(),True),
        StructField("watch_duration_sec",IntegerType(),True),
        StructField("abandoned_reason",StringType(),True)
    ]
)