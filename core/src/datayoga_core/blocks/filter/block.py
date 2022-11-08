import logging
from typing import Any, Dict, List

from datayoga_core import expression
from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context

logger = logging.getLogger("dy")


class Block(DyBlock):
    def init(self, context: Context = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.expression = expression.compile(self.properties["language"], self.properties["expression"])

    async def run(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        logger.debug(f"Running {self.get_block_name()}")
        return self.expression.filter(data)
