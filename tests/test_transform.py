

import logging
import os
from os import path

import datayoga as dy
from datayoga.job import Job
from datayoga.utils import read_yaml

logger = logging.getLogger(__name__)

TEST_DATA = [
    {
        "before": {"fname": "yossi", "lname": "shirizli", "country_code": 972, "country_name": "israel", "credit_card": "1234-5678-0000-9999"},
        "after":  {"first_name": "yossi", "last_name": "shirizli", "country": "972 - ISRAEL", "full_name": "yossi shirizli"}
    },
    {
        "before": {"fname": "oren", "lname": "elias", "country_code": 1, "country_name": "usa", "credit_card": "1234-5678-0000-9999"},
        "after":  {"first_name": "oren", "last_name": "elias", "country": "1 - USA", "full_name": "oren elias"}
    }
]


def test_transform_oo():
    job_settings = get_job_settings_from_test_yaml()
    job = Job(job_settings)

    for data in TEST_DATA:
        assert job.transform(data["before"]) == data["after"]


def test_compile_and_transform_module():
    job_settings = get_job_settings_from_test_yaml()
    job = dy.compile(job_settings)

    for data in TEST_DATA:
        assert job.transform(data["before"]) == data["after"]


def test_transform_module():
    job_settings = get_job_settings_from_test_yaml()

    for data in TEST_DATA:
        assert dy.transform(job_settings, data["before"]) == data["after"]


def get_job_settings_from_test_yaml():
    job_settings = read_yaml(path.join(os.path.dirname(os.path.realpath(__file__)), "test.yaml"))
    logger.debug(f"job_settings: {job_settings}")
    return job_settings
