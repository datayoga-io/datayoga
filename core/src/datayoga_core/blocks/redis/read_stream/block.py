import logging
from typing import Any, AsyncGenerator, Dict, List, Optional

import datayoga_core.blocks.redis.utils as redis_utils
import orjson
from datayoga_core.connection import Connection
from datayoga_core.context import Context
from datayoga_core.producer import Producer as DyProducer

logger = logging.getLogger("dy")


class Block(DyProducer):
    DEFAULT_FLUSH_MS = 1000

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

    async def produce_chunks(self) -> AsyncGenerator[List[Dict[str, Any]], None]:
        logger.debug(f"Running {self.get_block_name()}")
        batch_size = int(self.properties.get("batch_size", self.DEFAULT_BATCH_SIZE))
        read_pending = True

        while True:
            streams = self.redis_client.xreadgroup(
                self.consumer_group, self.requesting_consumer,
                {self.stream: "0" if read_pending else ">"},
                count=batch_size,
                block=100 if self.snapshot else 0,
            )

            yielded_any = False
            for stream in streams:
                logger.debug(f"Messages in {self.stream} stream (pending: {read_pending}):\n\t{stream}")
                chunk: List[Dict[str, Any]] = []
                for key, value in stream[1]:
                    payload = orjson.loads(value[next(iter(value))])
                    payload[self.MSG_ID_FIELD] = key
                    chunk.append(payload)
                if chunk:
                    yielded_any = True
                    yield chunk

            if self.snapshot and not read_pending and not yielded_any:
                return

            read_pending = False

    def ack(self, msg_ids: List[str]):
        for msg_id in msg_ids:
            logger.info(f"Acking {msg_id} message in {self.stream} stream of {self.consumer_group} consumer group")
            self.redis_client.xack(self.stream, self.consumer_group, msg_id)
