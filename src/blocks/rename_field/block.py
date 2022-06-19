import logging
from typing import Any

from datayoga.block import Block
from datayoga.context import Context

logger = logging.getLogger(__name__)


class RenameField(Block):
    def init(self):
        logger.debug(f"Initializing {self.get_block_name()}")

    def run(self, data: Any, context: Context = None) -> Any:
        logger.debug(f"Running {self.get_block_name()}")

        data[self.properties["to_field"]] = data.pop(self.properties["from_field"])
        return data
