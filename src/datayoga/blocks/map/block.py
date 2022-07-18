import json
import logging
from typing import Any

from datayoga.block import Block as DyBlock
from datayoga.blocks.expression import get_expression_class
from datayoga.context import Context

logger = logging.getLogger(__name__)


class Block(DyBlock):
    def init(self):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.expression = get_expression_class(self.properties["language"], json.dumps(self.properties["expression"]))

    def run(self, data: Any, context: Context = None) -> Any:
        logger.debug(f"Running {self.get_block_name()}")
        data = self.expression.search(data)

        return data
