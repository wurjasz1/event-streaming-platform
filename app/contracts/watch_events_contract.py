from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)

WATCH_EVENT_TYPES = {"play","pause",""}

WATCH_EVENT_SCHEMA = StructType(
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