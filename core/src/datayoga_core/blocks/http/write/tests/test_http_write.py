from typing import AsyncGenerator, List

import aiohttp
import mock
import pytest
import pytest_asyncio
from aiohttp import web
from aiohttp.test_utils import TestServer
from datayoga_core import Context, utils
from datayoga_core.blocks.http.write.block import Block


class RecordingServer(TestServer):
    """Local HTTP server that records the requests it receives."""
    requests: List[web.Request]
    bodies: List[str]


@pytest_asyncio.fixture
async def http_server() -> AsyncGenerator[RecordingServer, None]:
    """Fixture providing a local aiohttp server that answers PUT /users/{id}.

    Yields:
        RecordingServer: The running server, with the requests it received.
    """
    async def handler(request: web.Request) -> web.Response:
        server.requests.append(request)
        server.bodies.append(await request.text())
        return web.Response(status=200, body=b'{"status": "success"}',
                            headers={"my_header": "123", "Content-Type": "application/json"})

    app = web.Application()
    app.router.add_put("/users/{id}", handler)
    server = RecordingServer(app)
    server.requests = []
    server.bodies = []
    await server.start_server()
    yield server
    await server.close()


@pytest.mark.asyncio
async def test_http_write(http_server: RecordingServer):
    """Test case for the HTTP write block against a local HTTP server."""
    base_uri = str(http_server.make_url(""))
    context = Context({
        "connections": {
            "http_example": {
                "type": "http",
                "base_uri": base_uri,
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

    with mock.patch.object(aiohttp.ClientSession, "request", autospec=True,
                           side_effect=aiohttp.ClientSession.request) as request_spy:
        result = await block.run([
            {
                "id": 123,
                "credit_card": "1234-5678-0000-9999",
                "fname": "john",
                "lname": "doe",
                "country_name": "usa"
            }
        ])

    # the real server adds its own headers (Date, Server, Content-Length), so check ours separately
    response_headers = result.processed[0].payload["response"].pop("headers")
    assert response_headers["Content-Type"] == "application/json"
    assert response_headers["my_header"] == "123"

    assert result == utils.all_success([
        {
            "id": 123,
            "credit_card": "1234-5678-0000-9999",
            "fname": "john",
            "lname": "doe",
            "country_name": "usa",
            "response": {
                "status_code": 200,
                "content": '{"status": "success"}'
            }
        }
    ])

    # Validate the request received by the server
    assert len(http_server.requests) == 1
    request = http_server.requests[0]

    assert request.method == "PUT"
    assert request.path == "/users/123"

    # Validate the request headers
    assert request.headers["Authorization"] == "Bearer 1234-5678-0000-9999"
    assert request.headers["Content-Type"] == "application/json"
    assert request.headers["custom_header"] == "doe-john"

    # Validate the request query parameters
    assert request.query["country"] == "usa"
    assert request.query["origin_country"] == "israel"
    assert request.query["fname"] == "JOHN"

    # Validate the request body (data) and timeout passed to aiohttp
    request_spy.assert_called_once()
    assert request_spy.call_args.kwargs["data"] == {"full_name": "john doe"}
    assert request_spy.call_args.kwargs["timeout"] == 3
    assert http_server.bodies[0] == "full_name=john+doe"
