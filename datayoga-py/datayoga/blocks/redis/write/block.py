import logging
from functools import reduce
from typing import Any, Dict, List, Optional

import datayoga.blocks.redis.utils as utils
from datayoga.block import Block as DyBlock
from datayoga.context import Context
from datayoga.utils import get_connection_details

logger = logging.getLogger("dy")


class Block(DyBlock):
    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.command = self.properties.get("command")
        self.key_field = self.properties.get("key_field")
        connection = get_connection_details(self.properties.get("connection"), context)
        self.redis_client = utils.get_client(
            connection.get("host"),
            connection.get("port"),
            connection.get("password"))
        logger.info(f"Writing to Redis connection '{self.properties.get('connection')}'")

    async def run(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        pipeline = self.redis_client.pipeline()
        for record in data:
            dict_as_list = list(reduce(lambda x, y: x + y, record.items()))
            pipeline.execute_command(self.command, record[self.key_field], *dict_as_list)
        pipeline.execute()
