import logging
from abc import ABCMeta
from typing import Dict

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
    connection_details: Dict

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.connection_details = utils.get_connection_details(self.properties["bootstrap_servers"], context)

        self.bootstrap_servers = self.connection_details.get("bootstrap_servers")
        self.group = self.properties.get("group")
        self.topic = self.properties["topic"]
        self.seek_to_beginning = self.properties.get("seek_to_beginning", False)
        self.snapshot = self.properties.get("snapshot", False)


    async def produce(self) -> AsyncGenerator[List[Message], None]:
        consumer = Consumer(self._get_config())
        logger.debug(f"Producing {self.get_block_name()}")

        if self.seek_to_beginning:
            def on_assign(c, ps):
                for p in ps:
                    p.offset = -2
                c.assign(ps)
            consumer.subscribe([self.topic], on_assign)
        else:
            consumer.subscribe([self.topic])

        try:
            while True:
                # Poll for messages
                msg = consumer.poll(3.0 if self.snapshot else None)
                counter = next(count())
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
                    if counter % self.MIN_COMMIT_COUNT == 0:
                        consumer.commit(asynchronous=False)

        finally:
            consumer.close()

    def _get_config(self):
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group,
            'enable.auto.commit': 'false'
        }
        proto = self.connection_details.get("security.protocol")
        if proto:
            conf['security.protocol'] = proto
        # TODO: how to distinguish here different auth protocols in kafka?
        if proto.startswith("SASL_"):
            conf['sasl.mechanisms'] = self.connection_details["sasl.mechanisms"]
            conf['sasl.username'] = self.connection_details["sasl.username"]
            conf['sasl.password'] = self.connection_details["sasl.password"]
        return conf




