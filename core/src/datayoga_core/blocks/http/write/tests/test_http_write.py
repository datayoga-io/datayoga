import json
from typing import Generator

import pytest
from aioresponses import aioresponses
from datayoga_core import Context, utils
from datayoga_core.blocks.http.write.block import Block


@pytest.fixture
def mock_aioresponse() -> Generator[aioresponses, None, None]:
    """Fixture providing an aioresponses instance for mocking aiohttp requests.

    Yields:
        aioresponses: An instance of aioresponses for mocking aiohttp requests.
    """
    with aioresponses() as m:
        yield m


@pytest.mark.asyncio
async def test_http_write(mock_aioresponse: aioresponses):
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

    url = "https://datayoga.io/users/123?country=usa&fname=JOHN&origin_country=israel"

    mock_aioresponse.put(url,
                         status=200,
                         body=json.dumps({"status": "success"}),
                         headers={"my_header": "123", 'Content-Type': 'application/json'})

    assert await block.run([
        {
            "id": 123,
            "credit_card": "1234-5678-0000-9999",
            "fname": "john",
            "lname": "doe",
            "country_name": "usa"
        }
    ]) == utils.all_success([
        {
            "id": 123,
            "credit_card": "1234-5678-0000-9999",
            "fname": "john",
            "lname": "doe",
            "country_name": "usa",
            "response": {
                "status_code": 200,
                "headers": {
                    "Content-Type": "application/json",
                    "my_header": "123"
                },
                "content": '{"status": "success"}'
            }
        }
    ])

    # Validate the aiohttp request
    calls = mock_aioresponse.requests

    assert len(calls) == 1

    request_key = list(calls.keys())[0]
    request = calls[request_key][0].kwargs

    assert request_key[0] == "PUT"
    assert f"{request_key[1]}" == url

    # Validate the request headers
    assert request["headers"]["Authorization"] == "Bearer 1234-5678-0000-9999"
    assert request["headers"]["Content-Type"] == "application/json"
    assert request["headers"]["custom_header"] == "doe-john"

    # Validate the request query parameters
    assert request["params"]["country"] == "usa"
    assert request["params"]["origin_country"] == "israel"
    assert request["params"]["fname"] == "JOHN"

    # Validate the request body (data)
    assert request["data"] == {"full_name": "john doe"}
    assert request["timeout"] == 3
