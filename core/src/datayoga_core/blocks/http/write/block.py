import logging
from typing import Any, Dict, List, Optional

import requests
from datayoga_core import expression, utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context
from datayoga_core.expression import Expression
from datayoga_core.result import BlockResult, Result, Status

logger = logging.getLogger("dy")


class Block(DyBlock):
    base_uri: str
    method: str
    request_config: Dict[str, Any]
    response_status_code_field: Optional[str]
    response_headers_field: Optional[str]
    response_content_field: Optional[str]
    timeout: int

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

        connection_name = self.properties["connection"]
        connection_details = utils.get_connection_details(connection_name, context)
        if connection_details["type"] != "http":
            raise ValueError(f"{connection_name} is not an HTTP connection")

        self.base_uri = f"{connection_details['base_uri'].rstrip('/')}"
        self.method = self.properties["method"]
        output = self.properties.get("output", {})
        self.response_status_code_field = output.get("status_code")
        self.response_headers_field = output.get("headers")
        self.response_content_field = output.get("body")
        self.timeout = self.properties.get("timeout", connection_details.get("timeout", 10))

        def process_dict(input_dict, output_dict):
            for key, value in input_dict.items():
                if isinstance(value, dict) and "expression" in value and "language" in value:
                    output_dict[key] = expression.compile(value["language"], value["expression"])
                elif isinstance(value, dict):
                    output_dict[key] = {}
                    process_dict(value, output_dict[key])
                else:
                    output_dict[key] = value

        self.request_config = {}
        process_dict({
            "endpoint": self.properties["endpoint"],
            "headers": {**connection_details.get("headers", {}), **self.properties.get("extra_headers", {})},
            "query_params": {
                **connection_details.get("query_parameters", {}),
                **self.properties.get("extra_query_parameters", {})},
            "payload": self.properties.get("payload", {})
        }, self.request_config)

    async def run(self, data: List[Dict[str, Any]]) -> BlockResult:
        logger.debug(f"Running {self.get_block_name()}")

        block_result = BlockResult()

        def process_dict(input_dict, output_dict):
            for key, value in input_dict.items():
                if isinstance(value, Expression):
                    output_dict[key] = value.search_bulk(data)
                elif isinstance(value, dict):
                    output_dict[key] = {}
                    process_dict(value, output_dict[key])
                else:
                    output_dict[key] = [value] * len(data)

        request_configs = {}
        process_dict(self.request_config, request_configs)

        for i, row in enumerate(data):
            response = None

            try:
                url = f"{self.base_uri}/{request_configs['endpoint'][i].lstrip('/')}"
                headers = {key: value[i] for key, value in request_configs["headers"].items()}
                query_params = {key: value[i] for key, value in request_configs["query_params"].items()}
                payload = {key: value[i] for key, value in request_configs["payload"].items()}

                logger.debug(
                    f"Sending HTTP {self.method} request to: {url}\nheaders:{headers}\n\tquery_params: {query_params}\n\tpayload: {payload}")

                if self.method == "GET":
                    response = requests.get(url, params=query_params, headers=headers, timeout=self.timeout)
                elif self.method == "POST":
                    response = requests.post(url, data=payload, params=query_params,
                                             headers=headers, timeout=self.timeout)
                elif self.method == "PUT":
                    response = requests.put(url, data=payload, params=query_params,
                                            headers=headers, timeout=self.timeout)
                elif self.method == "DELETE":
                    response = requests.delete(url, params=query_params, headers=headers, timeout=self.timeout)

                if response:
                    logger.debug(f"HTTP response code: {response.status_code}")
                    logger.debug(f"Response Content: {response.content}")

                    if self.response_status_code_field:
                        utils.set_field(row, self.response_status_code_field, response.status_code)

                    if self.response_headers_field:
                        utils.set_field(row, self.response_headers_field, response.headers)

                    if self.response_content_field:
                        utils.set_field(row, self.response_content_field, response.content)

                    if response.ok:
                        block_result.processed.append(Result(Status.SUCCESS, payload=row))
                    else:
                        error_message = response.text if response else "Unknown error"
                        block_result.rejected.append(
                            Result(
                                status=Status.REJECTED, payload=row,
                                message=f"HTTP request failed with status code {response.status_code if response else 'Unknown'}. Error message: {error_message}"))
            except Exception as e:
                block_result.rejected.append(
                    Result(status=Status.REJECTED, payload=row, message=f"Error making HTTP request: {f'{e}'}"))

        return block_result
