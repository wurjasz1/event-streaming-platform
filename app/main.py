import time
import logging
from pathlib import Path

from app.generator.event_generator import EventGenerator
from app.generator.event_duplicator import EventDuplicator
from app.generator.event_corruptor import EventCorruptor
from app.generator.delay_buffer import DelayBuffer
from app.kafka_client.producer import KafkaEventProducer
from app.validator.watch_event_schema_validator import SchemaValidator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)

TOPIC = "watch-events"
BASE_DIR=Path(__file__).resolve().parent
SCHEMA_PATH=BASE_DIR/"schemas"/"watch_event_v1.json"
BOOTSTRAP_SERVERS = "localhost:9092"

def main():
    generator = EventGenerator()
    duplicator = EventDuplicator()
    corruptor = EventCorruptor()
    delay_buffer = DelayBuffer()
    schema_validator = SchemaValidator(SCHEMA_PATH)

    try:
        with KafkaEventProducer("localhost:9092") as producer:
            while True:
                logger.info("🚀 Starting event streaming pipeline")
                #generator returns raw data (dict)
                event = generator.generate_events()
                if event is None:
                    continue

                #duplicator
                events_after_duplication = duplicator.process(event)

                for item in events_after_duplication:

                    #serialization object to JSON ready dict
                    payload=item.to_dict()

                    #corupting payload on purpose
                    payload = corruptor.corrupt(payload)

                    #official schema validation (JSON Schema)
                    #if corruptor changes payload not according to the validation rules, it will raise the exception
                    try:
                        schema_validator.validate(payload)

                        logger.info(
                            "Schema validation passed, event_id=%s",
                            payload.get("event_id")
                        )
                    except Exception as e:
                        logger.error(
                            "Schema validation FAILED, error=%s, payload=%s",
                            str(e),
                            payload
                        )
                        continue

                    #delay buffer operates on payload dict
                    delayed_events = delay_buffer.process(payload)

                    for e in delayed_events:
                        key = e.get("session_id", "unknown")
                        producer.send(TOPIC, key, e)
                        logger.info(
                            "Sent event to producer buffer, key: %s, event_id=%s, event_type=%s",
                                    key,
                                    e.get("event_id"),
                                    e.get("event_type")
                                )

                time.sleep(0.5)

    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.exception("Aplication failed unexpectedly")
        raise

if __name__ == "__main__":
    main()