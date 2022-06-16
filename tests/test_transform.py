

import logging
import os
from os import path

import datayoga as dy
from datayoga.job import Job
from datayoga.utils import read_yaml

logger = logging.getLogger(__name__)

DATA_BEFORE = [
    {"fname": "yossi", "lname": "shirizli", "credit_card": "1234-5678-0000-9999"},
    {"fname": "oren", "lname": "elias", "country": "israel", "credit_card": "1234-5678-0000-9999"}
]

DATA_AFTER = [
    {"first_name": "yossi", "last_name": "shirizli", "full_name": "yossi shirizli"},
    {"first_name": "oren", "last_name": "elias", "country": "israel", "full_name": "oren elias"}
]


def test_transform_oo():
    job_yaml = get_job_yaml()
    logger.debug(f"job_yaml: {job_yaml}")
    job = Job(job_yaml)
    assert job.transform(DATA_BEFORE) == DATA_AFTER


def test_transform_module():
    job_yaml = get_job_yaml()
    job = dy.compile(job_yaml)

    assert dy.transform(job, DATA_BEFORE) == DATA_AFTER


def get_job_yaml():
    job_yaml = read_yaml(path.join(os.path.dirname(os.path.realpath(__file__)), "test.yaml"))
    logger.debug(f"job_yaml: {job_yaml}")
    return job_yaml
