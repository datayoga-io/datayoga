import logging
from typing import Any, Dict, List

from datayoga.block import Block as DyBlock
from datayoga.context import Context

logger = logging.getLogger(__name__)


class Block(DyBlock):
    def init(self):
        logger.debug(f"Initializing {self.get_block_name()}")

    def run(self, data: List[Dict[str, Any]], context: Context = None) -> List[Dict[str, Any]]:
        logger.debug(f"Running {self.get_block_name()}")

        for row in data:
            for property in self.properties:
                row[property["to_field"]] = row.pop(property["from_field"])

        return data
