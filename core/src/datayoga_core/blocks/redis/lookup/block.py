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
            connection.get("password"))

        self.command = self.properties.get("command", "GET")
        self.field = self.properties.get("field")
        self.reject_on_error = self.properties.get("reject_on_error", False)

        key = self.properties["key"]
        self.key_expression = expression.compile(key["language"], key["expression"])

        logger.info(f"Using Redis connection '{self.properties.get('connection')}'")

    async def run(self, data: List[Dict[str, Any]]) -> BlockResult:
        pipeline = self.redis_client.pipeline()
        block_result = BlockResult()

        for record in data:
            key = self.key_expression.search(record)

            if self.command == 'GET':
                pipeline.get(key)
            elif self.command == 'HGETALL':
                pipeline.hgetall(key)
            elif self.command == 'SMEMBERS':
                pipeline.smembers(key)
            elif self.command == 'ZRANGEBYSCORE':
                pipeline.zrangebyscore(key, "-inf", "+inf", withscores=True)
            elif self.command == 'LRANGE':
                pipeline.lrange(key, 0, -1)
            elif self.command == 'JSON.GET':
                pipeline.execute_command("JSON.GET", key)
            else:
                raise ValueError("Invalid command")

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
