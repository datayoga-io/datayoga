from typing import List

from kafka import KafkaProducer
from testcontainers.kafka import KafkaContainer
def get_kafka_container(port: int) -> KafkaContainer:
    return KafkaContainer().with_bind_ports(9092, port)

def get_kafka_producer(topic: str) -> KafkaProducer:
    return KafkaProducer()
