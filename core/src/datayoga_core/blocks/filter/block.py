import logging
from typing import Any, Dict, List, Tuple

from datayoga_core import expression, utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context
from datayoga_core.result import Result, Status

logger = logging.getLogger("dy")


class Block(DyBlock):
    def init(self, context: Context = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.expression = expression.compile(self.properties["language"], self.properties["expression"])

    async def run(self, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Result]]:
        logger.debug(f"Running {self.get_block_name()}")
        # all rows are filtered until proven otherwise
        is_filtered = {row[Block.MSG_ID_FIELD]:True for row in data}
        return_data = self.expression.filter(data)
        # mark filtered rows
        is_filtered.update({row[Block.MSG_ID_FIELD]:False for row in return_data})
        logger.warn([Result(Status.FILTERED) if filtered else Result(Status.SUCCESS) for filtered in is_filtered.values()])

        return return_data,[Result(Status.FILTERED) if filtered else Result(Status.SUCCESS) for filtered in is_filtered.values()]
