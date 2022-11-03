import ast
import os

from common.utils import run_job


def test_stdin_to_stdout(tmpdir: str):
    tested_data = {"id": 121, "fname": "joe", "lname": "allen",
                   "country_code": "US", "country_name": "united states", "gender": "M"}

    output_file = tmpdir.join("test_stdin_to_stdout.txt")
    run_job("test_stdin_to_srdout.yaml", f'echo "{tested_data}"', output_file)

    result = ast.literal_eval(output_file.read())

    assert result["full_name"] == "Joe Allen"
    assert result["country"] == "US - UNITED STATES"
    assert result.get("fname") is None
    assert result.get("lname") is None
    assert result.get("country_code") is None
    assert result.get("country_name") is None
    assert result["gender"] == "M"

    os.remove(output_file)
