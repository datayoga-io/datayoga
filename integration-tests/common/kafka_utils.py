from confluent_kafka import Producer
from testcontainers.kafka import KafkaContainer
def get_kafka_container() -> KafkaContainer:
    return KafkaContainer().with_bind_ports(KafkaContainer.KAFKA_PORT, KafkaContainer.KAFKA_PORT)

def get_kafka_producer(bootstrap_servers: str) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": "integration-tests"
    })
