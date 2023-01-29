import json
import logging
from typing import Any, Dict, List, Optional

from datayoga_core import expression
from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context
from datayoga_core.result import BlockResult, Result, Status

logger = logging.getLogger("dy")


class Block(DyBlock):
    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        expression_prop = json.dumps(
            self.properties["expression"]) if isinstance(
            self.properties["expression"],
            dict) else self.properties["expression"]
        # the expression is now a string (jmespath expressions are not valid JSON)
        expression_prop = expression_prop.strip()
        if not (expression_prop.startswith("{") and expression_prop.endswith("}")):
            raise ValueError("map expression must be in a json-like format enclosed in { }")
        self.expression = expression.compile(self.properties["language"], expression_prop)

    async def run(self, data: List[Dict[str, Any]]) -> BlockResult:
        logger.debug(f"Running {self.get_block_name()}")
        mapped_rows = self.expression.search_bulk(data)
        # we always add the internal fields back, in case they were dropped in the map operation
        for mapped_row, original_row in zip(mapped_rows, data):
            original_msg_id= original_row.get(Block.MSG_ID_FIELD)
            original_opcode= original_row.get(Block.OPCODE_FIELD)
            if original_msg_id is not None:
                mapped_row[Block.MSG_ID_FIELD] = original_msg_id
            if original_opcode is not None:
                mapped_row[Block.OPCODE_FIELD] = original_opcode

        return BlockResult(processed=[Result(Status.SUCCESS, payload=row) for row in mapped_rows])
