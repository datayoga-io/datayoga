import logging
import sys
from abc import ABCMeta
from typing import Any, Dict, List, Optional

import orjson
from datayoga_core import utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context
from datayoga_core.result import BlockResult
from datayoga_core.utils import remove_msg_id

logger = logging.getLogger("dy")


class Block(DyBlock, metaclass=ABCMeta):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

    async def run(self, data: List[Dict[str, Any]]) -> BlockResult:
        for record in data:
            filtered_record = remove_msg_id(record)
            sys.stdout.write(f"{orjson.dumps(filtered_record).decode()}\n")

        # if we made it here, it is a success. return the data and the success result
        return utils.all_success(data)
