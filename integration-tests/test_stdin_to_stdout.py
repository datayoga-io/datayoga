import json
import os

import pytest

from common.utils import run_job


def test_stdin_to_stdout(tmpdir):
    tested_data = '{"id": 121, "fname": "joe", "lname": "allen", "country_code": "US", "country_name": "united states", "gender": "M"}'

    output_file = tmpdir.join("test_stdin_to_stdout.txt")
    run_job("tests.stdin_to_stdout", f"echo '{tested_data}'", output_file)

    result = json.loads(output_file.read())

    assert result.get("full_name") == "Joe Allen"
    assert result.get("country") == "US - UNITED STATES"
    assert result.get("fname") is None
    assert result.get("lname") is None
    assert result.get("country_code") is None
    assert result.get("country_name") is None
    assert result.get("gender") == "M"

    os.remove(output_file)

def test_stdin_to_stdout_filtered(tmpdir):
    tested_data = '{"id": 121, "fname": "joe121", "lname": "allen", "country_code": "US", "country_name": "united states", "gender": "M"}'
    tested_data2 = '{"id": 122, "fname": "shmoe", "lname": "allen", "country_code": "US", "country_name": "united states", "gender": "M"}'
    tested_data3 = '{"id": 123, "fname": "joe123", "lname": "allen", "country_code": "US", "country_name": "united states", "gender": "M"}'

    output_file = tmpdir.join("test_stdin_to_stdout.txt")
    run_job("tests.stdin_to_stdout", f"echo '{tested_data}\n{tested_data2}\n{tested_data3}'", output_file)

    contents = open(output_file, "r").read()
    result = [json.loads(str(item)) for item in contents.strip().split('\n')]

    assert result == [
        {"id": 121,"full_name": "Joe121 Allen", "country": "US - UNITED STATES", "gender": "M"},
        {"id": 123,"full_name": "Joe123 Allen", "country": "US - UNITED STATES", "gender": "M"},
    ]
    os.remove(output_file)

