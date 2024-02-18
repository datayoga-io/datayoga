import json
import logging
from abc import ABCMeta
from itertools import count
from typing import AsyncGenerator, List, Optional

import orjson
from confluent_kafka import Consumer, KafkaError
from datayoga_core import utils
from datayoga_core.context import Context
from datayoga_core.producer import Message
from datayoga_core.producer import Producer as DyProducer

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
        logger.debug(f"Connection details: {json.dumps(connection_details)}")
        self.bootstrap_servers = connection_details.get("bootstrap_servers")
        self.group = self.properties.get("group")
        self.topic = self.properties["topic"]
        self.seek_to_beginning = self.properties.get("seek_to_beginning", False)
        self.snapshot = self.properties.get("snapshot", False)

    async def produce(self) -> AsyncGenerator[List[Message], None]:
        consumer = Consumer(**{
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group,
            'enable.auto.commit': False,
            'auto.offset.reset': 'earliest',
        })
        logger.debug(f"Producing {self.get_block_name()}")
        # consumer.assign([TopicPartition(self.topic, 0)])

        if self.seek_to_beginning:
            def on_assign(c, ps):
                for p in ps:
                    p.offset = -2
                c.assign(ps)

            consumer.subscribe([self.topic], on_assign)
            logger.debug(f"Seeking to beginning on topic {self.topic}"'')
        else:
            consumer.subscribe([self.topic])

        try:
            while True:
                # Poll for messages
                msg = consumer.poll(3.0 if self.snapshot else None)
                counter = next(count())
                if msg is None:
                    assert self.snapshot
                    logger.warning(f"Snapshot defined quitting on topic {self.topic}")
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
                    # res = consumer.get_watermark_offsets(TopicPartition(self.topic, 0))
                    # logger.error(res)
                    # consumer.commit(offsets=res, asynchronous=False)
                    if counter % self.MIN_COMMIT_COUNT == 0:
                        consumer.commit(asynchronous=False)

        finally:
            try:
                consumer.commit(asynchronous=False)
            except Exception as e:
                logger.error(e)
            consumer.close()
