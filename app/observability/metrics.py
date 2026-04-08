from prometheus_client import Counter, Histogram, Gauge

KAFKA_PRODUCER_QUEUED_TOTAL = Counter(
    "kafka_producer_queued_total",
    "Messages accepted into local producer queue"
)

KAFKA_PRODUCER_DELIVERED_TOTAL = Counter(
    "kafka_producer_delivered_total",
    "Messages successfully delivered to Kafka broker"
)

KAFKA_PRODUCER_DELIVERY_FAILED_TOTAL = Counter(
    "kafka_producer_delivery_failed_total",
    "Messages that failed delivery in Kafka callback"
)

KAFKA_PRODUCER_DLQ_DELIVERED_TOTAL = Counter(
    "kafka_producer_dlq_delivered_total",
    "Messages successfully delivered to DLQ topic"
)

KAFKA_PRODUCER_RETRY_TOTAL = Counter(
    "kafka_producer_retry_total",
    "Producer retry attempts"
)

KAFKA_PRODUCER_SPOOLED_TOTAL = Counter(
    "kafka_producer_spooled_total",
    "Messages written to local spool because Kafka was unavailable"
)

KAFKA_PRODUCER_SPOOL_REPLAYED_TOTAL = Counter(
    "kafka_producer_spool_replayed_total",
    "Messages replayed successfully from local spool"
)

KAFKA_PRODUCER_INFLIGHT = Gauge(
    "kafka_producer_inflight",
    "Messages waiting for delivery callback"
)

KAFKA_PRODUCER_BROKER_AVAILABLE = Gauge(
    "kafka_producer_broker_available",
    "Kafka broker availability state: 1 healthy, 0 degraded"
)

KAFKA_PRODUCER_CONSECUTIVE_FAILURES = Gauge(
    "kafka_producer_consecutive_failures",
    "Consecutive Kafka delivery failures"
)

KAFKA_PRODUCER_DELIVERY_SECONDS = Histogram(
    "kafka_producer_delivery_seconds",
    "End-to-end Kafka delivery latency in seconds"
)