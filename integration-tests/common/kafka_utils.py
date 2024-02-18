from kafka import KafkaProducer
from testcontainers.kafka import KafkaContainer


def get_kafka_container() -> KafkaContainer:
    return (
        KafkaContainer(
            image="confluentinc/cp-kafka:latest") .with_bind_ports(
            KafkaContainer.KAFKA_PORT,
            KafkaContainer.KAFKA_PORT))


def get_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(bootstrap_servers=bootstrap_servers)
