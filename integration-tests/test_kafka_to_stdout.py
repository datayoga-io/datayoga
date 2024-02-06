import logging
from common import kafka_utils
from common.utils import run_job

logger = logging.getLogger("dy")

def test_kafka_to_stdout():
    kafka_container = kafka_utils.get_kafka_container()
    with kafka_container as kafka:
        bootstrap_servers = kafka.get_bootstrap_server()
        logger.info(f"Connecting to Kafka: {bootstrap_servers}")
        producer = kafka_utils.get_kafka_producer(bootstrap_servers)
        producer.send("integration-tests", b"test")
        producer.flush()
        producer.close()
        run_job("tests.kafka_to_stdout")



