import logging
import os
from abc import ABCMeta
from typing import Generator, Optional

from datayoga_core.context import Context
from datayoga_core.producer import Message
from datayoga_core.producer import Producer as DyProducer
from fastparquet import ParquetFile

logger = logging.getLogger("dy")


class Block(DyProducer, metaclass=ABCMeta):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        parquet_file = self.properties["file"]

        if os.path.isabs(parquet_file) or context is None:
            self.file = parquet_file
        else:
            self.file = os.path.join(context.properties.get("data_path"), parquet_file)

        logger.debug(f"file: {self.file}")

    def produce(self) -> Generator[Message, None, None]:
        logger.debug("Reading parquet")

        pf = ParquetFile(self.file)

        count = 0
        for df in pf.iter_row_groups():
            for _, data in df.iterrows():
                yield {self.MSG_ID_FIELD: str(count), **data.to_dict()}
                count += 1
