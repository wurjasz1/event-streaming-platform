import time

from app.generator.event_generator import EventGenerator
from app.generator.event_duplicator import EventDuplicator
from app.generator.event_corruptor import EventCorruptor
from app.generator.delay_buffer import DelayBuffer
from app.kafka_client.producer import KafkaEventProducer

TOPIC = "watch-events"


def main():
    generator = EventGenerator()
    duplicator = EventDuplicator()
    corruptor = EventCorruptor()
    delay_buffer = DelayBuffer()
    producer = KafkaEventProducer()

    try:
        while True:
            event = generator.generate_events()
            events_after_duplication = duplicator.process(event)

            for item in events_after_duplication:
                payload=item.convert_to_dict()
                payload = corruptor.corrupt(payload)
                delayed_events = delay_buffer.process(payload)

                for e in delayed_events:
                    key = e.get("user_id", "unknown")
                    producer.send(TOPIC, key, e)
                    print("Sent:", e)

            time.sleep(0.5)

    except KeyboardInterrupt:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()