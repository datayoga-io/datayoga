import json
import logging
from typing import Generator, List, Optional

import datayoga_core.blocks.redis.utils as redis_utils
from datayoga_core.context import Context
from datayoga_core.producer import Message
from datayoga_core.producer import Producer as DyProducer
from datayoga_core.utils import get_connection_details

logger = logging.getLogger("dy")


class Block(DyProducer):
    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

        connection = get_connection_details(self.properties.get("connection"), context)
        self.redis_client = redis_utils.get_client(
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

    def produce(self) -> Generator[Message, None, None]:
        logger.debug(f"Running {self.get_block_name()}")

        while True:
            streams = self.redis_client.xreadgroup(
                self.consumer_group, self.requesting_consumer, {self.stream: ">"}, None, 0)
            for stream in streams:
                for key, value in stream[1]:
                    payload = json.loads(value[next(iter(value))])
                    payload[self.MSG_ID_FIELD] = key
                    yield payload

            if self.snapshot:
                break

    def ack(self, msg_ids: List[str]):
        for msg_id in msg_ids:
            logger.info(f"Acking {msg_id} message in {self.stream} stream of {self.consumer_group} consumer group")
            self.redis_client.xack(self.stream, self.consumer_group, msg_id)
