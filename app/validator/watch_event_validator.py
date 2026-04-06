from pyspark.sql import DataFrame, functions as F
from app.contracts.watch_events_contract import WATCH_EVENT_TYPES

def parse watch_event(df: DataFrame,)