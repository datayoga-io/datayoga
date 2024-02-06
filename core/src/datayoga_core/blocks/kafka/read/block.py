import logging
from abc import ABCMeta
from itertools import count

from datayoga_core.context import Context
from typing import AsyncGenerator, List, Optional
from datayoga_core.producer import Producer as DyProducer, Message
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from datayoga_core import utils

logger = logging.getLogger("dy")

import json


class Block(DyProducer, metaclass=ABCMeta):
    bootstrap_servers: List[str]
    group: str
    topic: str
    seek_to_beginning: bool = False
    INTERNAL_FIELD_PREFIX = "__$$"
    MSG_ID_FIELD = f"{INTERNAL_FIELD_PREFIX}msg_id"

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        connection_details = utils.get_connection_details(self.properties["bootstrap_servers"], context)

        self.port = int(connection_details.get("port", 9092))
        logger.debug(f"Connection details: {json.dumps(connection_details)}")
        self.bootstrap_servers = connection_details.get("bootstrap_servers", ["0.0.0.0"])
        self.group = connection_details.get("group")
        self.topic = connection_details.get("topic", "integration-tests")
        self.seek_to_beginning = self.properties.get("seek_to_beginning", False)

    async def produce(self) -> AsyncGenerator[List[Message], None]:
        logger.debug(f"Producing {self.get_block_name()}")

        if self.seek_to_beginning:
            logger.debug(f"Seeking to beginning...")
            consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_servers, group_id=self.group)
            tp = TopicPartition(self.topic, 0)
            consumer.assign([tp])
            consumer.seek_to_beginning()
        else:
            consumer = KafkaConsumer(self.topic, bootstrap_servers=self.bootstrap_servers, group_id=self.group)

        if not consumer.bootstrap_connected():
            raise KafkaError("Unable to connect with kafka container!")
        for message in consumer:
            counter = iter(count())
            logger.debug(f"Received message: {message}")
            yield [{self.MSG_ID_FIELD: f"{next(counter)}", **message}]