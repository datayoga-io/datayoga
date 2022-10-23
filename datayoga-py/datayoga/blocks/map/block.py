import json
import logging
from typing import Any, Dict, List

from datayoga.block import Block as DyBlock
from datayoga import expression
from datayoga.context import Context

logger = logging.getLogger("dy")


class Block(DyBlock):
    def init(self, context: Context = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.expression = expression.compile(self.properties["language"], json.dumps(self.properties["expression"]))

    async def run(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        logger.debug(f"Running {self.get_block_name()}")
        return_data = []
        for row in data:
            return_data.append(self.expression.search(row))

        return return_data
