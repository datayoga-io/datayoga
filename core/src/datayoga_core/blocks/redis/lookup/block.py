import logging
from abc import ABCMeta
from typing import Any, Dict, List, Optional

import datayoga_core.blocks.redis.utils as redis_utils
import redis
from datayoga_core import expression
from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context
from datayoga_core.result import BlockResult, Result, Status
from datayoga_core.utils import get_connection_details

logger = logging.getLogger("dy")


class Block(DyBlock, metaclass=ABCMeta):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

        connection = get_connection_details(self.properties.get("connection"), context)
        self.redis_client = redis_utils.get_client(
            connection.get("host"),
            connection.get("port"),
            connection.get("password")
        )

        self.field = self.properties.get("field")
        self.reject_on_error = self.properties.get("reject_on_error", False)

        cmd = self.properties["cmd"]
        self.cmd_expression = expression.compile(self.properties["language"], self.properties["cmd"])

        args = self.properties["args"]
        self.args_expressions = [expression.compile(self.properties["language"], c) for c in args]

        logger.info(f"Using Redis connection '{self.properties.get('connection')}'")

    async def run(self, data: List[Dict[str, Any]]) -> BlockResult:
        pipeline = self.redis_client.pipeline()
        block_result = BlockResult()

        for record in data:
            params = [self.cmd_expression.search(record)]
            for e in (c.search(record) for c in self.args_expressions):
                params.extend(e if isinstance(e, list) else [e])

            pipeline.execute_command(*params)

        try:
            results = pipeline.execute(raise_on_error=False)
            for record, result in zip(data, results):
                exc = isinstance(result, Exception)
                if exc and self.reject_on_error:
                    block_result.rejected.append(Result(Status.REJECTED, message=f"{result}", payload=record))
                else:
                    record[self.field] = None if exc else result
                    block_result.processed.append(Result(Status.SUCCESS, payload=record))
        except redis.exceptions.ConnectionError as e:
            raise ConnectionError(e)

        return block_result
