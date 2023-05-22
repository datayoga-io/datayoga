import logging
import os
from abc import ABCMeta
from contextlib import suppress
from csv import DictReader
from itertools import count, islice
from typing import AsyncGenerator, List, Optional

from datayoga_core.context import Context
from datayoga_core.producer import Message
from datayoga_core.producer import Producer as DyProducer

logger = logging.getLogger("dy")


class Block(DyProducer, metaclass=ABCMeta):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        csv_file = self.properties["file"]

        if os.path.isabs(csv_file) or context is None:
            self.file = csv_file
        else:
            self.file = os.path.join(context.properties.get("data_path"), csv_file)

        logger.debug(f"file: {self.file}")

        self.encoding = self.properties.get("encoding", "utf-8")
        self.batch_size = self.properties.get("batch_size", 1000)
        self.fields = self.properties.get("fields")
        self.skip = self.properties.get("skip", 0)
        self.delimiter = self.properties.get("delimiter", ",")
        self.quotechar = self.properties.get("quotechar", "\"")

    async def produce(self) -> AsyncGenerator[List[Message], None]:
        logger.debug("Reading CSV")

        with open(self.file, "r", encoding=self.encoding) as read_obj:
            reader = DictReader(read_obj, fieldnames=self.fields, delimiter=self.delimiter, quotechar=self.quotechar)
            counter = iter(count())

            for _ in range(self.skip):
                with suppress(StopIteration):
                    next(reader)

            while True:
                sliced = islice(reader, self.batch_size)
                records = [{self.MSG_ID_FIELD: f"{next(counter)}", **record} for record in sliced]

                if not records:
                    logger.debug(f"Done reading {self.file}")
                    return

                logger.debug(f"Producing {len(records)} records")

                yield records
