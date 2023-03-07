import json
import logging
from abc import ABCMeta
from typing import Any, Dict, List, Optional, Union

from datayoga_core import expression
from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context
from datayoga_core.expression import Language
from datayoga_core.result import BlockResult, Result, Status

logger = logging.getLogger("dy")


class Block(DyBlock, metaclass=ABCMeta):

    @staticmethod
    def _prepare_expression(language: Language, expr: Union[dict, str]) -> str:
        if language == Language.SQL:
            return json.dumps(expr) if isinstance(expr, dict) else expr.strip()

        if isinstance(expr, dict):
            expr = ", ".join([f"{k}: {v}" for k, v in expr.items()])
            expr = f"{{{expr}}}"

        expr = expr.strip()

        # jmespath expression for map block must be enclosed in { }
        if not (expr.startswith("{") and expr.endswith("}")):
            raise ValueError("map expression must be in a json-like format enclosed in { }")

        return expr

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

        language = self.properties["language"]
        expression_prop = self._prepare_expression(language, self.properties["expression"])

        self.expression = expression.compile(language, expression_prop)

    async def run(self, data: List[Dict[str, Any]]) -> BlockResult:
        logger.debug(f"Running {self.get_block_name()}")
        mapped_rows = self.expression.search_bulk(data)
        # we always add the internal fields back, in case they were dropped in the map operation
        for mapped_row, original_row in zip(mapped_rows, data):
            original_msg_id = original_row.get(Block.MSG_ID_FIELD)
            original_opcode = original_row.get(Block.OPCODE_FIELD)
            if original_msg_id is not None:
                mapped_row[Block.MSG_ID_FIELD] = original_msg_id
            if original_opcode is not None:
                mapped_row[Block.OPCODE_FIELD] = original_opcode

        return BlockResult(processed=[Result(Status.SUCCESS, payload=row) for row in mapped_rows])
