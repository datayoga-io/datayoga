import logging
from abc import ABCMeta
from typing import Any, Dict, List, Optional

from datayoga_core import expression
from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context
from datayoga_core.result import BlockResult, Result, Status

logger = logging.getLogger("dy")


class Block(DyBlock, metaclass=ABCMeta):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.expression = expression.compile(self.properties["language"], self.properties["expression"])

    async def run(self, data: List[Dict[str, Any]]) -> BlockResult:
        result = BlockResult()
        logger.debug(f"Running {self.get_block_name()}")
        return_data = self.expression.filter(data, tombstone=True)
        # mark filtered rows
        for i, row in enumerate(return_data):
            if row is None:
                result.filtered.append(Result(Status.FILTERED, payload=data[i]))
            else:
                result.processed.append(Result(Status.SUCCESS, payload=row))

        return result
