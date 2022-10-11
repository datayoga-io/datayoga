

import logging

import datayoga as dy
import pytest
from common.utils import get_job_settings_from_yaml
from datayoga.job import Job

logger = logging.getLogger("dy")

TEST_DATA = [
    {
        "before": [{"fname": "yossi", "lname": "shirizli", "country_code": 972, "country_name": "israel", "credit_card": "1234-5678-0000-9999", "gender": "M"}],
        "after":  [{"first_name": "yossi", "last_name": "shirizli", "country": "972 - ISRAEL", "full_name": "yossi shirizli", "greeting": "Hello Mr. yossi shirizli"}]
    },
    {
        "before": [{"fname": "jane", "lname": "smith", "country_code": 1, "country_name": "usa", "credit_card": "1234-5678-0000-9999", "gender": "F"}],
        "after":  [{"first_name": "jane", "last_name": "smith", "country": "1 - USA", "full_name": "jane smith", "greeting": "Hello Ms. jane smith"}]
    }
]

TEST_YAML = "test.yaml"


def test_transform_oo():
    job_settings = get_job_settings_from_yaml(TEST_YAML)
    job = Job(job_settings)

    for data in TEST_DATA:
        assert job.transform(data["before"]) == data["after"]


def test_compile_and_transform_module():
    job_settings = get_job_settings_from_yaml(TEST_YAML)
    job = dy.compile(job_settings)

    for data in TEST_DATA:
        assert job.transform(data["before"]) == data["after"]


def test_transform_module():
    job_settings = get_job_settings_from_yaml(TEST_YAML)

    for data in TEST_DATA:
        assert dy.transform(job_settings, data["before"]) == data["after"]


def test_validate_valid_job():
    job_settings = get_job_settings_from_yaml(TEST_YAML)
    dy.validate(job_settings)


def test_validate_invalid_job():
    # `with` key is missing in this block
    job_settings = [{"uses": "add_field"}]

    with pytest.raises(ValueError):
        dy.validate(job_settings)
