import logging
from typing import Any

from datayoga.block import Block as DyBlock
from datayoga.context import Context

logger = logging.getLogger(__name__)


class Block(DyBlock):
    def init(self):
        logger.debug(f"Initializing {self.get_block_name()}")

    def run(self, data: Any, context: Context = None) -> Any:
        logger.debug(f"Running {self.get_block_name()}")

        del data[self.properties["field"]]
        return data
