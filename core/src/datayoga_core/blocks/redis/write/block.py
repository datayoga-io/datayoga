import logging
from typing import Any, Dict, List, Optional, Tuple

import datayoga_core.blocks.redis.utils as redis_utils
import redis
from datayoga_core import expression, utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.block import Result
from datayoga_core.context import Context
from datayoga_core.utils import get_connection_details

logger = logging.getLogger("dy")


class Block(DyBlock):
    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

        connection = get_connection_details(self.properties.get("connection"), context)
        self.redis_client = redis_utils.get_client(
            connection.get("host"),
            connection.get("port"),
            connection.get("password"))

        self.command = self.properties.get("command", "HSET")

        key = self.properties["key"]
        self.key_expression = expression.compile(key["language"], key["expression"])

        logger.info(f"Writing to Redis connection '{self.properties.get('connection')}'")

    async def run(self, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Result]]:
        pipeline = self.redis_client.pipeline()
        for record in data:
            # transform to a list, filtering it out None, which Redis does not support
            dict_as_list = sum(filter(
                lambda i: i[1] is not None and not i[0].startswith(Block.INTERNAL_FIELD_PREFIX),
                record.items()
            ),())
            pipeline.execute_command(self.command, self.key_expression.search(record), *dict_as_list)

        try:
            results = pipeline.execute(raise_on_error=False)
            for record, result in zip(data, results):
                if isinstance(result, Exception):
                    utils.reject_record(f"{result}", record)
        except redis.exceptions.ConnectionError as e:
            raise ConnectionError(e)

        return utils.produce_data_and_results(data)
