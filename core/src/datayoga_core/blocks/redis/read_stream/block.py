import logging
from typing import Any, AsyncGenerator, Dict, List, Optional

import datayoga_core.blocks.redis.utils as redis_utils
import orjson
from datayoga_core.connection import Connection
from datayoga_core.context import Context
from datayoga_core.producer import Producer as DyProducer

logger = logging.getLogger("dy")


class Block(DyProducer):
    """Producer block that reads messages from a Redis stream consumer group."""

    DEFAULT_FLUSH_MS = 1000

    def init(self, context: Optional[Context] = None):
        """Connects to Redis and ensures the consumer group exists on the target stream."""
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
        """Reads pending then new stream messages via XREADGROUP, yielding each response as a chunk.

        Pending entries (id="0") are drained in a single unbounded XREADGROUP
        call (count=None) — this matches pre-PR behavior. Paginating PEL via
        count is not safe with a non-acking producer because XREADGROUP id="0"
        always returns from the start of PEL, so a smaller count would just
        re-read the same first page forever. New-message reads (id=">") use
        count=batch_size to bound the Redis network response size.
        """
        logger.debug(f"Running {self.get_block_name()}")
        batch_size = int(self.properties.get("batch_size", self.DEFAULT_BATCH_SIZE))
        read_pending = True

        while True:
            streams = self.redis_client.xreadgroup(
                self.consumer_group, self.requesting_consumer,
                {self.stream: "0" if read_pending else ">"},
                count=None if read_pending else batch_size,
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

            # Flip unconditionally after the first pending-read call: count=None
            # drained the entire PEL in that single call, so there's no more
            # pending work to do this session.
            read_pending = False

    def ack(self, msg_ids: List[str]):
        """Acknowledges the given message ids with XACK on the stream consumer group."""
        for msg_id in msg_ids:
            logger.info(f"Acking {msg_id} message in {self.stream} stream of {self.consumer_group} consumer group")
            self.redis_client.xack(self.stream, self.consumer_group, msg_id)
