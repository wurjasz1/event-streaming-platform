import json
from confluent_kafka import Producer
import logging

logger = logging.getLogger(__name__)

class KafkaEventProducer:
    def __init__(self,bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer=None

    def __enter__(self):
        self.producer = Producer(
            {
                'bootstrap.servers': self.bootstrap_servers,
                "client.id": "event-streaming-platform-producer",
                "acks": "all"
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
            logger.info("Kafka producer closed")

        #do not hide exception
        return False

    def _delivery_callback(self,err,msg):
        if err is not None:
            logger.error(
                "Delivery failed, topic=%s, key=%s, error=%s",
                msg.topic() if msg else None,
                msg.key().decode("utf-8",errors="ignore") if msg and msg.key() else None,
                err
            )
        else:
            logger.info(
                "Delivered topic=%s, partition=%s, offset=%s, key=%s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                msg.key().decode("utf-8",errors="ignore") if  msg.key() else None
            )

    def send(self,topic:str, key:str, value:dict) -> None:
        if self.producer is None:
            raise RuntimeError("Producer not initialized")

        try:
            self.producer.produce(
                topic=topic,
                key=key.encode('utf-8'),
                value=json.dumps(value).encode('utf-8'),
                callback=self._delivery_callback
            )

            self.producer.poll(0)

        except BufferError as e:
            logger.exception(
                "Local producer queue is full, topic=%s, key=%s",
                topic,
                key
            )
            self.producer.poll(1)

            self.producer.produce(
                topic=topic,
                key=key.encode('utf-8'),
                value=json.dumps(value).encode('utf-8'),
                callback=self._delivery_callback
            )

            self.producer.poll(0)

        except Exception:
            logger.exception(
                "Producer failed before broker ack, topic=%s, key=%s",
                topic,
                key
            )
            raise

    # methods for quitting the infinite loop
    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.close()