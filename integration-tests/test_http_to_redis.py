import json
import os
from contextlib import suppress
from time import sleep

import requests

from common import redis_utils
from common.utils import wait_program, run_job

REDIS_PORT = 12554
DY_URL = "http://localhost:8080"


def test_http_to_redis():
    redis_container = redis_utils.get_redis_oss_container(REDIS_PORT)

    try:
        redis_container.start()
        program = run_job("tests.http_to_redis", background=True)

        for i in range(30):
            with suppress(requests.exceptions.ConnectionError):
                requests.post(DY_URL)
                break
            sleep(0.5)
        else:
            raise ValueError("Can't wait any longer for the process to be ready!")

        file_name = os.path.join(os.path.dirname(os.path.realpath(__file__)), "resources", "data", "employees.json")
        with open(file_name, "r") as f:
            for record in json.load(f):
                requests.post(DY_URL, data=json.dumps(record))

        sleep(2)

        redis_client = redis_utils.get_redis_client("localhost", REDIS_PORT)

        assert len(redis_client.keys()) == 3

        first_employee = redis_client.hgetall("1")
        assert first_employee["id"] == "1"
        assert first_employee["full_name"] == "John Doe"
        assert first_employee["country"] == "972 - ISRAEL"
        assert first_employee["gender"] == "M"

        second_employee = redis_client.hgetall("2")
        assert second_employee["id"] == "2"
        assert second_employee["full_name"] == "Jane Doe"
        assert second_employee["country"] == "972 - ISRAEL"
        assert second_employee["gender"] == "F"

        third_employee = redis_client.hgetall("3")
        assert third_employee["id"] == "3"
        assert third_employee["full_name"] == "Bill Adams"
        assert third_employee["country"] == "1 - USA"
        assert third_employee["gender"] == "M"

    finally:
        with suppress(Exception):
            redis_container.stop()
        with suppress(Exception):
            wait_program(program, ignore_errors=True)  # noqa
