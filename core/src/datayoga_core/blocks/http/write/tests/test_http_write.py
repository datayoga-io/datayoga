import json
from typing import Any, Dict

import pytest
from datayoga_core import Context, utils
from datayoga_core.blocks.http.write.block import Block
from pytest_mock import MockerFixture


class MockResponse:
    def __init__(self, status_code: int, json_data: Any, headers: Dict[str, Any]):
        self.status_code = status_code
        self.json_data = json_data
        self.headers = headers
        self.content = json.dumps(json_data).encode("utf-8")
        self.ok = 200 <= status_code < 300

    def json(self):
        return self.json_data


@pytest.mark.asyncio
async def test_http_write(mocker: MockerFixture):
    """Test case for the HTTP write block with mocked requests.put."""
    context = Context({
        "connections": {
            "http_example": {
                "type": "http",
                "base_uri": "https://datayoga.io",
                "headers": {
                        "Authorization": {
                            "expression": "concat(['Bearer ', credit_card])",
                            "language": "jmespath"
                        },
                    "Content-Type": "application/json"
                },
                "query_parameters": {
                    "country": {
                        "expression": "country_name",
                        "language": "jmespath"
                    },
                    "origin_country": "israel"
                },
                "timeout": 12
            }
        }
    })

    block = Block({
        "connection": "http_example",
        "endpoint": {
            "expression": "concat(['users/', id])",
            "language": "jmespath"
        },
        "method": "PUT",
        "payload": {
            "full_name": {
                "expression": "concat([fname, ' ' , lname])",
                "language": "jmespath"
            },
        },
        "extra_headers": {
            "custom_header": {
                "expression": "lname || '-' || fname",
                "language": "sql"
            }
        },
        "extra_query_parameters": {
            "fname": {
                "expression": "UPPER(fname)",
                "language": "sql"
            }
        },
        "output": {
            "status_code": "response.status_code",
            "headers": "response.headers",
            "body": "response.content"
        },
        "timeout": 3
    })

    block.init(context)

    mock_response = {
        "status": "success"
    }

    mock_put = mocker.patch("requests.put", return_value=MockResponse(
        status_code=200, json_data=mock_response, headers={"my_header": 123}))

    assert await block.run([
        {"id": 123, "credit_card": "1234-5678-0000-9999", "fname": "john", "lname": "doe", "country_name": "usa"}
    ]) == utils.all_success([
        {"id": 123,  "credit_card": "1234-5678-0000-9999", "fname": "john", "lname": "doe", "country_name": "usa", "response": {"status_code": 200, "headers": {"my_header": 123}, "content": b'{"status": "success"}'}}
    ])

    mock_put.assert_called_once_with(
        "https://datayoga.io/users/123",
        headers={
            "Authorization": "Bearer 1234-5678-0000-9999",
            "Content-Type": "application/json",
            "custom_header": "doe-john",
        },
        params={
            "country": "usa",
            "origin_country": "israel",
            "fname": "JOHN"
        },
        data={
            "full_name": "john doe",
        },
        timeout=3
    )
