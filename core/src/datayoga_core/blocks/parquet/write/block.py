import logging
import os
from abc import ABCMeta
from typing import Any, Dict, List, Optional

from datayoga_core import utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context
from datayoga_core.result import BlockResult
from datayoga_core.utils import remove_msg_id
from fastparquet import write
from pandas import DataFrame

logger = logging.getLogger("dy")


class Block(DyBlock, metaclass=ABCMeta):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        parquet_file = self.properties["file"]

        if os.path.isabs(parquet_file) or context is None:
            self.file = parquet_file
        else:
            self.file = os.path.join(context.properties.get("data_path"), parquet_file)

        logger.debug(f"file: {self.file}")

    async def run(self, data: List[Dict[str, Any]]) -> BlockResult:
        logger.debug("Writing parquet")

        out = [{"data": remove_msg_id(record)} for record in data]
        append = os.path.exists(self.file)
        write(self.file, DataFrame(out), append=append, has_nulls=True)

        # if we made it here, it is a success. return the data and the success result
        return utils.all_success(data)
