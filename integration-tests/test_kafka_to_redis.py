import json
import logging

from common import kafka_utils, redis_utils
from common.utils import run_job

logger = logging.getLogger("dy")
message_one = b'{"id":"1","name":"Boris"}'
message_two = b'{"id":"2","name":"Ivan"}'


def test_kafka_to_redis():
    kafka_container = kafka_utils.get_kafka_container()
    try:
        with kafka_container as kafka:
            redis_container = redis_utils.get_redis_oss_container(redis_utils.REDIS_PORT)
            redis_container.start()

            bootstrap_servers = kafka.get_bootstrap_server()
            producer = kafka_utils.get_kafka_producer(bootstrap_servers)
            producer.send("integration-tests", message_one)
            producer.send("integration-tests", message_two)
            producer.flush()
            run_job("tests.kafka_to_redis")

            redis_client = redis_utils.get_redis_client("localhost", redis_utils.REDIS_PORT)

            assert len(redis_client.keys()) == 2

            boris = redis_client.hgetall("1")
            ivan = redis_client.hgetall("2")

            assert boris == json.loads(message_one.decode())
            assert ivan == json.loads(message_two.decode())
    finally:
        redis_container.stop()
