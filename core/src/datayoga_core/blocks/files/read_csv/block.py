import logging
import os
from abc import ABCMeta
from contextlib import suppress
from csv import DictReader
from itertools import count, islice
from typing import Any, AsyncGenerator, Dict, List, Optional

from datayoga_core.context import Context
from datayoga_core.producer import Producer as DyProducer

logger = logging.getLogger("dy")


class Block(DyProducer, metaclass=ABCMeta):
    """Producer block that reads records from a CSV file."""

    def init(self, context: Optional[Context] = None):
        """Initializes the block: resolves the CSV file path and reader options."""
        logger.debug(f"Initializing {self.get_block_name()}")
        csv_file = self.properties["file"]
        if os.path.isabs(csv_file) or context is None:
            self.file = csv_file
        else:
            self.file = os.path.join(context.properties.get("data_path"), csv_file)
        logger.debug(f"file: {self.file}")
        self.encoding = self.properties.get("encoding", "utf-8")
        self.fields = self.properties.get("fields")
        self.skip = self.properties.get("skip", 0)
        self.delimiter = self.properties.get("delimiter", ",")
        self.quotechar = self.properties.get("quotechar", "\"")

    async def produce_chunks(self) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """Yields successive `batch_size`-sized chunks of CSV rows."""
        logger.debug("Reading CSV")
        batch_size = int(self.properties.get("batch_size", self.DEFAULT_BATCH_SIZE))

        with open(self.file, "r", encoding=self.encoding) as read_obj:
            reader = DictReader(read_obj, fieldnames=self.fields,
                                delimiter=self.delimiter, quotechar=self.quotechar)
            for _ in range(self.skip):
                with suppress(StopIteration):
                    next(reader)
            counter = iter(count())
            while True:
                chunk = [
                    {self.MSG_ID_FIELD: f"{next(counter)}", **record}
                    for record in islice(reader, batch_size)
                ]
                if not chunk:
                    return
                yield chunk
