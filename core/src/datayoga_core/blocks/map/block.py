import json
import logging
from typing import Any, Dict, List, Tuple

from datayoga_core import expression
from datayoga_core.block import Block as DyBlock
from datayoga_core.block import Result
from datayoga_core.context import Context

logger = logging.getLogger("dy")


class Block(DyBlock):
    def init(self, context: Context = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.expression = expression.compile(self.properties["language"], json.dumps(self.properties["expression"]))

    async def run(self, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Result]]:
        logger.debug(f"Running {self.get_block_name()}")
        results = []
        return_data = []
        for row in data:
            mapped_row = self.expression.search(row)
            # we always add the msg_id back
            if Block.MSG_ID_FIELD in row:
                mapped_row[Block.MSG_ID_FIELD] = row[Block.MSG_ID_FIELD]
            return_data.append(mapped_row)

        results.append(Result.SUCCESS)
        return return_data, results
