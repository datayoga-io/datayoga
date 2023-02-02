import logging
import os
import textwrap
from os import path

import datayoga_core as dy
import pytest
import yaml
from jsonschema import ValidationError

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
        assert job.transform(data["before"]).processed[0].payload == data["after"][0]


def test_compile_and_transform_module(job_settings):
    job = dy.compile(job_settings)
    job.init()

    for data in TEST_DATA:
        assert job.transform(data["before"]).processed[0].payload == data["after"][0]


def test_transform_module(job_settings):
    for data in TEST_DATA:
        assert dy.transform(job_settings, data["before"]).processed[0].payload == data["after"][0]


def test_validate_valid_job(job_settings):
    dy.validate(job_settings)


def test_block_after_filter():
    job_yaml = """
        steps:
          - uses: filter
            with:
                expression: fname='firstfname'
                language: sql
          - uses: map
            with:
                expression:
                    id: id
                language: sql
    """
    data = [{"fname": "firstfname", "id": 1}, {"fname": "secondfname", "id": 2}]
    processed, filtered, rejected = dy.transform(yaml.safe_load(textwrap.dedent(job_yaml)), data)
    assert [result.payload for result in processed] == [{"id": 1}]
    assert rejected == []
    assert [result.payload for result in filtered] == [{"fname": "secondfname", "id": 2}]


def test_block_after_filter_empty_results():
    job_yaml = """
        steps:
          - uses: filter
            with:
                expression: fname='x'
                language: sql
          - uses: add_field
            with:
                field: field0
                expression: fname || 'x'
                language: sql
    """
    data = [{"fname": "firstfname", "id": 1}]
    processed, filtered, rejected = dy.transform(yaml.safe_load(textwrap.dedent(job_yaml)), data)
    assert [result.payload for result in processed] == []
    assert rejected == []
    assert [result.payload for result in filtered] == [{"fname": "firstfname", "id": 1}]


def test_validate_invalid_job():
    # unsupported property specified in this block
    job_settings = {"steps": [{"uses": "remove_field", "with": {"field": "my_field", "bla": "xxx"}}]}

    with pytest.raises(ValueError):
        dy.validate(job_settings)


def test_block_not_in_whitelisted_blocks(job_settings):
    with pytest.raises(ValidationError):
        dy.compile(job_settings, whitelisted_blocks=["add_field", "rename_field", "remove_field"])
