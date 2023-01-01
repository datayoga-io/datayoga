import logging
from typing import Any, Dict, List, Optional, Tuple

from datayoga_core import expression
from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context
from datayoga_core.result import Result, Status

logger = logging.getLogger("dy")


class Block(DyBlock):
    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.expression = expression.compile(self.properties["language"], self.properties["expression"])

    async def run(self, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Result]]:
        logger.debug(f"Running {self.get_block_name()}")
        return_data = self.expression.filter(data, tombstone=True)
        # mark filtered rows
        return [x for x in return_data if x is not None], [Result(Status.FILTERED) if x is None else Result(Status.SUCCESS) for x in return_data]
