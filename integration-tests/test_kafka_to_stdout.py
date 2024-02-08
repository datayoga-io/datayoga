import logging
import os

from common import kafka_utils
from common.utils import run_job

logger = logging.getLogger("dy")
message_one = b'{"id":1,"name":"Boris"}'
message_two = b'{"id":2,"name":"Ivan"}'

def test_kafka_to_stdout(tmpdir):
    kafka_container = kafka_utils.get_kafka_container()
    try:
        with kafka_container as kafka:
            bootstrap_servers = kafka.get_bootstrap_server()
            producer = kafka_utils.get_kafka_producer(bootstrap_servers)
            producer.produce("integration-tests", message_one)
            producer.produce("integration-tests", message_two)
            producer.flush()
            output_file = tmpdir.join("tests_kafka_to_stdout.txt")
            run_job("tests.kafka_to_stdout", None, output_file)
            result = output_file.readlines()
            assert result[0].strip().encode() == message_one
            assert result[1].strip().encode() == message_two
    finally:
        os.remove(output_file)



