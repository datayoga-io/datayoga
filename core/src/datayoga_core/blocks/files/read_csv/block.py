import logging
import os
from csv import DictReader
from typing import Generator, Optional

from datayoga_core.context import Context
from datayoga_core.producer import Message
from datayoga_core.producer import Producer as DyProducer

logger = logging.getLogger("dy")


class Block(DyProducer):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        csv_file = self.properties["file"]

        if os.path.isabs(csv_file) or context is None:
            self.file = csv_file
        else:
            self.file = os.path.join(context.properties.get("data_path"), csv_file)

        logger.debug(f"file: {self.file}")
        self.batch_size = self.properties.get("batch_size", 1000)

    def produce(self) -> Generator[Message, None, None]:
        logger.debug("Reading CSV")

        with open(self.file, "r") as read_obj:
            records = list(DictReader(read_obj))

        for i, record in enumerate(records):
            yield {self.MSG_ID_FIELD: str(i), **record}
