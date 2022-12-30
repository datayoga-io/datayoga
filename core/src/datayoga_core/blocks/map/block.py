import json
import logging
from typing import Any, Dict, List, Tuple

from datayoga_core import expression, utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.block import Result
from datayoga_core.context import Context

logger = logging.getLogger("dy")


class Block(DyBlock):
    def init(self, context: Context = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        expression_prop = json.dumps(
            self.properties["expression"]) if isinstance(
            self.properties["expression"],
            dict) else self.properties["expression"]
        self.expression = expression.compile(self.properties["language"], expression_prop)

    async def run(self, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Result]]:
        logger.debug(f"Running {self.get_block_name()}")
        return_data = []
        for row in data:
            mapped_row = self.expression.search(row)

            # we always add the internal fields back
            internal_fields = [(k, v) for (k, v) in row.items() if k.startswith(Block.INTERNAL_FIELD_PREFIX)]
            for (key, value) in internal_fields:
                mapped_row[key] = value

            return_data.append(mapped_row)

        return utils.all_success(return_data)
