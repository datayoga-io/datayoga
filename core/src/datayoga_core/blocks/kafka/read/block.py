import logging
import json
from abc import ABCMeta

from datayoga_core.context import Context
from typing import AsyncGenerator, List, Optional
from datayoga_core.producer import Producer as DyProducer, Message
from confluent_kafka import Consumer, KafkaError

from itertools import count
from datayoga_core import utils
import orjson

logger = logging.getLogger("dy")


class Block(DyProducer, metaclass=ABCMeta):
    bootstrap_servers: str
    group: str
    topic: str
    seek_to_beginning: bool
    snapshot: bool
    INTERNAL_FIELD_PREFIX = "__$$"
    MSG_ID_FIELD = f"{INTERNAL_FIELD_PREFIX}msg_id"
    MIN_COMMIT_COUNT = 10

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        connection_details = utils.get_connection_details(self.properties["bootstrap_servers"], context)

        self.port = int(connection_details.get("port", 9092))
        logger.debug(f"Connection details: {json.dumps(connection_details)}")
        self.bootstrap_servers = connection_details.get("bootstrap_servers")
        self.group = connection_details.get("group")
        self.topic = connection_details.get("topic", "integration-tests")
        self.seek_to_beginning = self.properties.get("seek_to_beginning", False)
        self.snapshot = self.properties.get("snapshot", False)
        self.consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group,
            'enable.auto.commit': 'false'
        })

    async def produce(self) -> AsyncGenerator[List[Message], None]:
        logger.debug(f"Producing {self.get_block_name()}")

        if self.seek_to_beginning:
            def on_assign(c, ps):
                for p in ps:
                    p.offset = -2
                c.assign(ps)
            self.consumer.subscribe([self.topic], on_assign)
        else:
            self.consumer.subscribe([self.topic])

        try:
            while True:
                # Poll for messages
                msg = self.consumer.poll(3.0 if self.snapshot else None)
                if msg is None:
                    assert self.snapshot
                    logger.warning(f"Snapshot defined quitting on topic {self.topic}"'')
                    break
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        logger.info("Reached end of partition")
                    else:
                        # Handle other errors
                        logger.error("Error: {}".format(msg.error()))

                else:
                    # Process the message
                    message = orjson.loads(msg.value())
                    yield [{self.MSG_ID_FIELD: msg.offset(), **message}]

        finally:
            self.consumer.close()

    def ack(self, msg_ids: List[str]):
        try:
            self.consumer.commit(asynchronous=False)
        except Exception as e:
            logger.error(f"Cannot commit: {e}")


