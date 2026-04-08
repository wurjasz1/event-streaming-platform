import json

from confluent_kafka import Producer
import logging

from app.fallback.spool import LocalSpool
from app.observability.metrics import (
    KAFKA_PRODUCER_QUEUED_TOTAL,
    KAFKA_PRODUCER_DELIVERED_TOTAL,
    KAFKA_PRODUCER_DELIVERY_FAILED_TOTAL,
    KAFKA_PRODUCER_DLQ_DELIVERED_TOTAL,
    KAFKA_PRODUCER_RETRY_TOTAL,
    KAFKA_PRODUCER_SPOOLED_TOTAL,
    KAFKA_PRODUCER_SPOOL_REPLAYED_TOTAL,
    KAFKA_PRODUCER_INFLIGHT,
    KAFKA_PRODUCER_BROKER_AVAILABLE,
    KAFKA_PRODUCER_CONSECUTIVE_FAILURES,
    KAFKA_PRODUCER_DELIVERY_SECONDS
)

from prometheus_client import start_http_server
from typing import Any
import uuid
import threading
import time

logger = logging.getLogger(__name__)

class KafkaEventProducer:
    def __init__(
            self,
            bootstrap_servers: str,
            main_topic: str = "watch-events",
            dlq_topic: str = "watch-events-dlq",
            max_buffer_retries: int=3,
            failure_threshold: int=5,
            metrics_port:int=8000,
            spool_path: str = "spool.jsonl"
    ):
        self.bootstrap_servers = bootstrap_servers
        self.main_topic=main_topic
        self.dlq_topic=dlq_topic
        self.producer=None
        self.max_buffer_retries=max_buffer_retries
        self.failure_threshold=failure_threshold

        self.metrics_port=metrics_port
        self.metrics_started = False
        self.producer:Producer | None=None

        self.spool = LocalSpool(filepath=spool_path)

        self.broker_available = True
        self.consecutive_failures = 0

        #message_id - timestamp
        self.inflight: dict[str,float] = {}

        self.recovery_thread: threading.Thread | None=None
        self.recovery_running=False


    def __enter__(self):
        if not self.metrics_started:
            start_http_server(self.metrics_port)
            self.metrics_started = True

        self.producer = Producer(
            {
                'bootstrap.servers': self.bootstrap_servers,
                "client.id": "event-streaming-platform-producer",
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "snappy",
                "linger.ms": 5,
                "batch.num.messages":1000,
                "message.timeout.ms":30000,
                "request.timeout.ms":15000
            }
        )

        self._mark_broker_healthy()

        logger.info("Kafka producer started")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        #Cleanup runs always
        try:
            if self.producer is not None:
                remaining=self.producer.flush(10)

                if remaining > 0:
                    logger.error(
                        "Producer closed with %s undelivered message(s)",
                        remaining
                    )
                else:
                    logger.info("Producer flushed successfully")
        finally:
            self.producer = None

        #do not hide exception
        return False

    def _mark_broker_healthy(self):
        self.broker_available = True
        KAFKA_PRODUCER_BROKER_AVAILABLE.set(1)

    def _extract_message_id(self, msg) -> str | None:
        if msg is None or msg.headers() is None:
            return None

        for key, value in msg.headers():
            if key == "message_id":
                return value.decode("utf-8") if isinstance(value, bytes) else value

        return None

    def _mark_broker_degraded(self):
        self.broker_available = False
        KAFKA_PRODUCER_BROKER_AVAILABLE.set(0)

    def _register_success(self, message_id: str | None):
        KAFKA_PRODUCER_DELIVERED_TOTAL.inc()

        self.consecutive_failures =0
        KAFKA_PRODUCER_CONSECUTIVE_FAILURES.set(0)
        self._mark_broker_healthy()

        if message_id is not None:
            start_ts = self.inflight.pop(message_id, None)
            if start_ts is not None:
                KAFKA_PRODUCER_INFLIGHT.dec()
                KAFKA_PRODUCER_DELIVERY_SECONDS.observe(time.time() - start_ts)

        if self.consecutive_failures >= self.failure_threshold:
            self._mark_broker_degraded()

    def _register_failure(self, message_id:str | None):
        KAFKA_PRODUCER_DELIVERY_FAILED_TOTAL.inc()

        self.consecutive_failures +=1
        KAFKA_PRODUCER_CONSECUTIVE_FAILURES.set(self.consecutive_failures)

        if message_id is not None:
            start_ts = self.inflight.pop(message_id,None)
            if start_ts is not None:
                KAFKA_PRODUCER_INFLIGHT.dec()
                KAFKA_PRODUCER_DELIVERY_SECONDS.observe(time.time()-start_ts)

        if self.consecutive_failures>=self.failure_threshold:
            self._mark_broker_degraded()

    def _delivery_callback(self, err, msg):
        message_id = self._extract_message_id(msg)

        if err is not None:
            self._register_failure(message_id)
            logger.error(
                "❌ Delivery failed, topic=%s, key=%s, error=%s",
                msg.topic() if msg else None,
                msg.key().decode("utf-8", errors="ignore") if msg and msg.key() else None,
                err
            )
        else:
            self._register_success(message_id)
            logger.debug(
                "Delivered topic=%s, partition=%s, offset=%s, key=%s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                msg.key().decode("utf-8", errors="ignore") if msg.key() else None
            )


    def _dlq_delivery_callback(self, err, msg):
        if err is not None:
            KAFKA_PRODUCER_DELIVERY_FAILED_TOTAL.inc()
            logger.error(
                "❌ DLQ Delivery failed, topic=%s, key=%s, error=%s",
                msg.topic() if msg else None,
                msg.key().decode("utf-8", errors="ignore") if msg and msg.key() else None,
                err
            )
        else:
            KAFKA_PRODUCER_DLQ_DELIVERED_TOTAL.inc()
            logger.warning(
                "DLQ Delivered topic=%s, partition=%s, offset=%s, key=%s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                msg.key().decode("utf-8", errors="ignore") if msg.key() else None
            )

    def _produce_once(self, topic:str, key:str, value:dict[str,Any]):
        if self.producer is None:
            raise RuntimeError("Producer not initialized")

        message_id = str(uuid.uuid4())
        payload_bytes = json.dumps(value).encode('utf-8')

        #Inflight register message_id - time
        self.inflight[message_id]=time.time()
        KAFKA_PRODUCER_INFLIGHT.inc()

        self.producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=payload_bytes,
            headers={"message_id": message_id},
            callback=self._delivery_callback
        )

        self.producer.poll(0)
        KAFKA_PRODUCER_QUEUED_TOTAL.inc()

        logger.info(
            "Event queued for async delivery, topic=%s, key=%s",
            topic,
            key
        )

    def spool_locally(self, topic:str,key:str,value=dict[str,Any],reason=str):
        record={
            "topic": topic,
            "key": key,
            "value": value,
            "reason": reason,
            "timestamp": time.time()
        }

        self.spool.append(record)
        KAFKA_PRODUCER_SPOOLED_TOTAL.inc()

        logger.error(
            "Message written to local spool, topic=%s, key=%s, reason=%s",
            topic,
            key,
            reason
        )

    def send(self,topic:str, key:str, value:dict) -> None:
        if self.producer is None:
            raise RuntimeError("Producer not initialized")

        if not self.broker_available:
            logger.error(
                "Broker degraded, writing to local spool, topic=%s, key=%s",
                topic,
                key
            )
            self.spool_locally(topic,key,value,reason="broker_unavailable")
            return

        for attempt in range(1,self.max_buffer_retries+1):
            try:
                self._produce_once(topic=topic,key=key,value=value)
                return

            except BufferError:
                KAFKA_PRODUCER_RETRY_TOTAL.inc()

                logger.warning(
                    "Local producer queue full, retrying, topic=%s, key=%s, attempt=%s/",
                    topic,
                    key,
                    attempt
                )
                self.producer.poll(1)
                time.sleep(min(0.5 * attempt, 2.0))
            except Exception as e:
                KAFKA_PRODUCER_RETRY_TOTAL.inc()

                logger.exception(
                    "Produce failed before broker ack, topic=%s, key=%s, attempt=%s/",
                    topic,
                    key,
                    attempt
                )
                if attempt<self.max_buffer_retries:
                    time.sleep(min(0.5*attempt,2.0))
                else:
                    self.spool_locally(topic,key,value,reason=str(e))
                    return

    def send_to_dlq(self, original_topic: str, key:str, value:dict[str,Any], error:str) -> None:
        #Send event to dlq with reason
        if self.producer is None:
            raise RuntimeError("Producer is not initialized")

        dlg_payload = {
            "original_topic": original_topic,
            "key": key,
            "payload": value,
            "error": error,
            "timestamp": time.time()
        }

        if not self.broker_available:
            logger.error(
                "Broker unavailable, writing DLQ payload to local spool, original_topic=%s, key=%s",
                original_topic,
                key
            )

            self.spool_locally(
                topic=self.dlq_topic,
                key=key,
                value = dlg_payload,
                reason = "broker_unavailable_for_dlq"
            )
            return

        try:
            self.producer.produce(
                topic=self.dlq_topic,
                key=key.encode("utf-8"),
                value=json.dumps(dlg_payload).encode('utf-8'),
                callback=self._dlq_delivery_callback
            )
            self.producer.poll(0)

            logger.warning(
                "Event queued for DlQ, dlq_topic=%s, original_topic=%s. key=%s,reason=%s",
                self.dlq_topic,
                original_topic,
                key
            )

        except Exception as e:
            KAFKA_PRODUCER_DELIVERY_FAILED_TOTAL.inc()
            logger.exception(
                "DQL produced Failed before brocker ack, dlq_topic=%s, original_topic=%s,key=%s",
                self.dlq_topic,
                original_topic,
                key
            )

            self.spool_locally(
                topic=self.dlq_topic,
                key=key,
                value = dlg_payload,
                reason = f"DLQ produce failed: {str(e)}"
            )

    def start_recovery_loop(self):
        if self.recovery_running:
            return

        self.recovery_running = True

        def loop():
            while self.recovery_running:
                try:
                    if self.broker_available:
                        had_records=False
                        replay_failed = False

                        for batch in self.spool.read_batches(batch_size=100):
                            had_records=True
                            logger.warning(
                                "Replaying batch from local spool, batch_size=%s",
                                len(batch)
                            )

                            for record in batch:
                                try:
                                    self._produce_once(
                                        topic=record["topic"],
                                        key=record["key"],
                                        value=record["value"]
                                    )
                                    KAFKA_PRODUCER_SPOOL_REPLAYED_TOTAL.inc()

                                except Exception:
                                    logger.exception("replay from spool failed, spool retained")
                                    replay_failed = True
                                    break

                            if replay_failed:
                                break

                        if had_records and not replay_failed:
                            self.spool.clear()
                            logger.warning("Local spool cleared after replay")

                    time.sleep(5)

                except Exception:
                    logger.exception("Recovery loop failed")
                    time.sleep(5)

        self.recovery_thread = threading.Thread(target=loop)
        self.recovery_thread.start()