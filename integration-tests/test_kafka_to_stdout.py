import logging

from common import kafka_utils
from common.utils import run_job

logger = logging.getLogger("dy")

def test_kafka_to_stdout(tmpdir):
    kafka_container = kafka_utils.get_kafka_container()
    with kafka_container as kafka:
        bootstrap_servers = kafka.get_bootstrap_server()
        logger.info(f"Connecting to Kafka: {bootstrap_servers}")
        producer = kafka_utils.get_kafka_producer(bootstrap_servers)
        producer.produce("integration-tests", b'{"hello": "Earth"}')
        producer.produce("integration-tests", b'{"bye": "Mars"}')
        producer.flush()
        output_file = tmpdir.join("tests_kafka_to_stdout.txt")
        run_job("tests.kafka_to_stdout", None, output_file)
        logger.error(output_file.read())
        # result = json.loads(output_file.read())
        # assert result[0]["hello"] == "Earth"
        # assert result[1]["bye"] == "Mars"



