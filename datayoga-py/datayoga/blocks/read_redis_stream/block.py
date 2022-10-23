import json
import logging
from typing import Any, Dict, List, Optional

import datayoga.blocks.read_redis_stream.utils as utils
from datayoga.block import Block as DyBlock
from datayoga.context import Context
from datayoga.utils import get_connection_details

logger = logging.getLogger("dy")


class Block(DyBlock):
    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

        connection = get_connection_details(self.properties.get("connection"), context)
        self.redis_client = utils.get_client(
            connection.get("host"),
            connection.get("port"),
            connection.get("password"))

        self.stream = self.properties["stream_name"]
        self.snapshot = self.properties.get("snapshot", False)
        self.consumer_group = f'datayoga_job_{context.properties.get("job_name", "") if context else ""}'
        self.requesting_consumer = "dy_consumer_a"
        stream_groups = self.redis_client.xinfo_groups(self.stream)
        if next(filter(lambda x: x["name"] == self.consumer_group, stream_groups), None) is None:
            logger.info(f"Creating a new {self.consumer_group} consumer group associated with the {self.stream}")
            self.redis_client.xgroup_create(self.stream, self.consumer_group, 0)

    async def run(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        logger.debug(f"Running {self.get_block_name()}")

        while True:
            streams = self.redis_client.xreadgroup(
                self.consumer_group, self.requesting_consumer, {self.stream: ">"}, None, 0)
            for stream in streams:
                for stream_element in stream[1]:
                    key, value = stream_element
                    yield {"key": key, "value": json.loads(value[next(iter(value))])}

            if self.snapshot:
                break

    def ack(self, key: str):
        logger.info(f"Acking {key} message in {self.stream} stream of {self.consumer_group} consumer group")
        self.redis_client.xack(self.stream, self.consumer_group, key)
