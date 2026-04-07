import json
from confluent_kafka import Producer
import logging

logger = logging.getLogger(__name__)

class KafkaEventProducer:
    def __init__(self,bootstrap_servers: str, max_buffer_retries: int=3):
        self.bootstrap_servers = bootstrap_servers
        self.producer=None
        self.max_buffer_retries=max_buffer_retries

        self.sent_count=0
        self.retry_count=0
        self.error_count=0
        self.dlq_count=0


    def __enter__(self):
        self.producer = Producer(
            {
                'bootstrap.servers': self.bootstrap_servers,
                "client.id": "event-streaming-platform-producer",
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "snappy",
                "linger.ms": 5,
                "batch.num.messages":1000
            }
        )
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
            logger.info(
                "Kafka producer closed, sent=%s, retry=%s, error=%s, dlw=%s",
                self.sent_count,
                self.retry_count,
                self.error_count,
                self.dlq_count
            )

        #do not hide exception
        return False

    def _delivery_callback(self,err,msg):
        if err is not None:
            logger.error(
                "❌ Delivery failed, topic=%s, key=%s, error=%s",
                msg.topic() if msg else None,
                msg.key().decode("utf-8",errors="ignore") if msg and msg.key() else None,
                err
            )
        else:
            logger.debug(
                "Delivered topic=%s, partition=%s, offset=%s, key=%s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                msg.key().decode("utf-8",errors="ignore") if  msg.key() else None
            )

    def send(self,topic:str, key:str, value:dict) -> None:
        if self.producer is None:
            raise RuntimeError("Producer not initialized")

        payload = json.dumps(value).encode('utf-8')
        encoded_key = key.encode('utf-8')

        attempt=0
        while True:
            try:
                self.producer.produce(
                    topic=topic,
                    key=encoded_key,
                    value=payload,
                    callback=self._delivery_callback
                )
                self.producer.poll(0)
                self.sent_count += 1
                return

            except BufferError as e:
                attempt+=1
                self.retry_count += 1

                logger.warning(
                    "Local producer queue is full, topic=%s, key=%s, attempt=%s/%s",
                    topic,
                    key,
                    attempt,
                    self.max_buffer_retries
                )
                self.producer.poll(1)

                if attempt>=self.max_buffer_retries:
                    self.error_count += 1
                    logger.error(
                        "Max buffer retry limit reached topic=%s, key=%s",
                        topic,
                        key
                    )
                    raise


                self.producer.poll(0)

            except Exception:
                self.error_count += 1
                logger.exception(
                    "Producer failed before broker ack, topic=%s, key=%s",
                    topic,
                    key
                )
                raise
    def send_to_dlq(self, dlq_topic: str, key:str, value:dict, reason:str) -> None:
        #Send event to dlq with reason
        if self.producer is None:
            raise RuntimeError("Producer is not initialized")

        dlg_payload = {
            "original_key": key,
            "original_event": value,
            "dql_reason": reason
        }

        try:
            self.producer.produce(
                topic=dlq_topic,
                key=key.encode("utf-8"),
                value=json.dumps(dlg_payload).encode('utf-8'),
                callback=self._delivery_callback
            )
            self.producer.poll(0)
            self.dlq_count += 1

            logger.error(
                "Event sent to DlQ, dlq_topic=%s, key=%s,reason=%s",
                dlq_topic,
                key,
                reason
            )
        except Exception:
            self.error_count += 1
            logger.exception(
                "Failed to send event to DLQ, dlq_topic=%s, key=%s",
                dlq_topic,
                key
            )
            raise
