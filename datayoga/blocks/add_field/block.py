import logging
from typing import Any, Dict, List

from datayoga.block import Block as DyBlock
from datayoga.blocks import expression
from datayoga.context import Context

logger = logging.getLogger(__name__)


class Block(DyBlock):
    def init(self):
        logger.debug(f"Initializing {self.get_block_name()}")
        for property in self.properties:
            property["compiled_expression"] = expression.compile(property["language"], property["expression"])

    def run(self, data: List[Dict[str, Any]], context: Context = None) -> List[Dict[str, Any]]:
        logger.debug(f"Running {self.get_block_name()}")

        for row in data:
            for property in self.properties:
                row[property["field"]] = property["compiled_expression"].search(row)

        return data
