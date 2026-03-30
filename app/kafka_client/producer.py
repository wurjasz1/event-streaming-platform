import json
from confluent_kafka import Producer

class KafkaEventProducer:
    def __init__(self,bootstrap_servers="localhost:9092"):
        self.producer= Producer({
            "bootstrap.servers" : bootstrap_servers
        }
        )
    def send(self,topic:str, key:str, value:dict):
        self.producer.produce(
            topic,
            key=key.encode('utf-8'),
            value=json.dumps(value).encode('utf-8')
        )

    # methods for quitting the infinite loop
    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.close()