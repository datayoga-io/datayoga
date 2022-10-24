import json
import logging
from typing import Any, Dict, List, Tuple

from datayoga.block import Block as DyBlock, Result
from datayoga import expression
from datayoga.context import Context

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
            return_data.append(self.expression.search(row))

        results.append(Result.SUCCESS)
        return return_data, results
