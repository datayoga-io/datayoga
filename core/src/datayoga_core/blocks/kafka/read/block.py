import logging
from abc import ABCMeta
from itertools import count

from datayoga_core.context import Context
from typing import AsyncGenerator, List, Optional
from datayoga_core.producer import Producer as DyProducer, Message
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logger = logging.getLogger("dy")


class Block(DyProducer, metaclass=ABCMeta):
    port: int
    bootstrap_servers: List[str]
    group: str
    topics: List[str]
    INTERNAL_FIELD_PREFIX = "__$$"
    MSG_ID_FIELD = f"{INTERNAL_FIELD_PREFIX}msg_id"

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.port = int(self.properties.get("port", 9092))
        self.bootstrap_servers = self.properties.get("bootstrap_servers", ["0.0.0.0"])
        self.group = self.properties.get("group", "integration-tests")
        self.topics = self.properties.get("topics", ["integration-tests"])

    async def produce(self) -> AsyncGenerator[List[Message], None]:
        logger.debug(f"Producing {self.get_block_name()}")
        consumer = KafkaConsumer(group_id=self.group, bootstrap_servers=self.bootstrap_servers)
        if not consumer.bootstrap_connected():
            raise KafkaError("Unable to connect with kafka container!")
        consumer.subscribe(self.topics)
        for message in consumer:
            counter = iter(count())
            yield [{self.MSG_ID_FIELD: f"{next(counter)}", **message}]