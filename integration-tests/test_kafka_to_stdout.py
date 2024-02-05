import logging
from common import kafka_utils
from common.utils import run_job

logger = logging.getLogger("dy")

def test_kafka_to_stdout():
    kafka_container = kafka_utils.get_kafka_container(9093)
    with kafka_container as kafka:
        connection = kafka.get_bootstrap_server()
        logger.info(f"Connecting to Kafka: {connection}")
        producer = kafka_utils.get_kafka_producer("integration-tests")
        producer.send("integration-tests", b"test", b"test")
        run_job("tests.kafka_to_stdout")

