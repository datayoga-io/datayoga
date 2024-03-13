import logging
from abc import ABCMeta
from typing import Any, Dict, List, Optional

import datayoga_core.blocks.redis.utils as redis_utils
import redis
from datayoga_core import expression, utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.connection import Connection
from datayoga_core.context import Context
from datayoga_core.result import BlockResult, Result, Status

logger = logging.getLogger("dy")


class Block(DyBlock, metaclass=ABCMeta):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

        connection_details = Connection.get_connection_details(self.properties.get("connection"), context)

        # Dry mode is internal and used for validate the block without establishing a connection.
        # This behavior should be implemented in a common way, see this issue: https://lnk.pw/eklj
        if not self.properties.get("dry"):
            self.redis_client = redis_utils.get_client(connection_details)

        self.field_path = [utils.unescape_field(field) for field in utils.split_field(self.properties.get("field"))]

        self.cmd = self.properties["cmd"]
        args = self.properties["args"]
        self.args_expressions = [expression.compile(self.properties["language"], c) for c in args]

        logger.info(f"Using Redis connection '{self.properties.get('connection')}'")

    async def run(self, data: List[Dict[str, Any]]) -> BlockResult:
        logger.debug(f"Running {self.get_block_name()}")

        pipeline = self.redis_client.pipeline(transaction=False)
        block_result = BlockResult()

        for record in data:
            params = [self.cmd]
            for expr in (c.search(record) for c in self.args_expressions):
                params.extend(expr if isinstance(expr, list) else [expr])

            pipeline.execute_command(*params)

        try:
            results = pipeline.execute(raise_on_error=False)
            for record, result in zip(data, results):
                if isinstance(result, Exception):
                    block_result.rejected.append(Result(Status.REJECTED, message=f"{result}", payload=record))
                    continue

                obj = record
                for field in self.field_path[:-1]:
                    obj = obj.setdefault(field, {})

                obj[self.field_path[-1]] = result

                block_result.processed.append(Result(Status.SUCCESS, payload=record))
        except redis.exceptions.ConnectionError as expr:
            raise ConnectionError(expr)

        return block_result
