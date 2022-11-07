import logging
import os
from os import path

import datayoga_core as dy
import pytest
import yaml

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


@pytest.fixture
def job_settings():
    filename = path.join(os.path.dirname(os.path.realpath(__file__)), "resources", "test.yaml")
    with open(filename, "r", encoding="utf8") as stream:
        job_settings = yaml.safe_load(stream)

    return job_settings


def test_transform_oo(job_settings):
    job = dy.compile(job_settings)
    job.init()

    for data in TEST_DATA:
        assert job.transform(data["before"]) == data["after"]


def test_compile_and_transform_module(job_settings):
    job = dy.compile(job_settings)
    job.init()

    for data in TEST_DATA:
        assert job.transform(data["before"]) == data["after"]


def test_transform_module(job_settings):
    for data in TEST_DATA:
        assert dy.transform(job_settings, data["before"]) == data["after"]


def test_validate_valid_job(job_settings):
    dy.validate(job_settings)


def test_validate_invalid_job():
    # unsupported property specified in this block
    job_settings = {"steps": [{"uses": "remove_field", "params": {"field": "my_field"}}]}

    with pytest.raises(ValueError):
        dy.validate(job_settings)


def test_block_not_in_whitelisted_blocks(job_settings):
    with pytest.raises(ValueError, match="map"):
        dy.compile(job_settings, whitelisted_blocks=["add_field", "rename_field", "remove_field"])
