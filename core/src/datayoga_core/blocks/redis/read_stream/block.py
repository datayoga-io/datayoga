import logging
from typing import AsyncGenerator, List, Optional

import datayoga_core.blocks.redis.utils as redis_utils
import orjson
from datayoga_core.connection import Connection
from datayoga_core.context import Context
from datayoga_core.producer import Message
from datayoga_core.producer import Producer as DyProducer

logger = logging.getLogger("dy")


class Block(DyProducer):
    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

        connection_details = Connection.get_connection_details(self.properties["connection"], context)
        self.redis_client = redis_utils.get_client(connection_details)

        self.stream = self.properties["stream_name"]
        self.snapshot = self.properties.get("snapshot", False)
        self.consumer_group = f'datayoga_job_{context.properties.get("job_name", "") if context else ""}'
        self.requesting_consumer = "dy_consumer_a"
        stream_groups = self.redis_client.xinfo_groups(self.stream)
        if next(filter(lambda x: x["name"] == self.consumer_group, stream_groups), None) is None:
            logger.info(f"Creating a new {self.consumer_group} consumer group associated with the {self.stream}")
            self.redis_client.xgroup_create(self.stream, self.consumer_group, 0)

    async def produce(self) -> AsyncGenerator[List[Message], None]:
        logger.debug(f"Running {self.get_block_name()}")

        read_pending = True
        while True:
            # Read pending messages (fetched by us before but not acknowledged) in the first time, then consume new messages
            streams = self.redis_client.xreadgroup(self.consumer_group, self.requesting_consumer, {
                self.stream: "0" if read_pending else ">"}, None, 100 if self.snapshot else 0)

            for stream in streams:
                logger.debug(f"Messages in {self.stream} stream (pending: {read_pending}):\n\t{stream}")
                for key, value in stream[1]:
                    payload = orjson.loads(value[next(iter(value))])
                    payload[self.MSG_ID_FIELD] = key
                    yield [payload]

            # Quit after consuming pending current messages in case of snapshot
            if self.snapshot and not read_pending:
                break

            read_pending = False

    def ack(self, msg_ids: List[str]):
        for msg_id in msg_ids:
            logger.info(f"Acking {msg_id} message in {self.stream} stream of {self.consumer_group} consumer group")
            self.redis_client.xack(self.stream, self.consumer_group, msg_id)
