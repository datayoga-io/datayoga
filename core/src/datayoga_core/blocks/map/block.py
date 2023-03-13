import logging
from abc import ABCMeta
from typing import Any, Dict, List, Optional, Union

import orjson
from datayoga_core import expression
from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context
from datayoga_core.expression import Language
from datayoga_core.result import BlockResult, Result, Status

logger = logging.getLogger("dy")


class Block(DyBlock, metaclass=ABCMeta):

    @staticmethod
    def _prepare_expression(language: Language, expr: Union[dict, str]) -> str:
        """Prepares the expression to compile.
        The sql expression will be dumped as json.
        For the jmespath the expression will be generated."""

        if language == Language.SQL:
            return orjson.dumps(expr).decode() if isinstance(expr, dict) else expr.strip()

        def prepare_key(key: str) -> str:
            return f'"{key}"' if " " in key else key

        # If there is an object here, we generate an expression.
        if isinstance(expr, dict):
            expr = ", ".join(f"{prepare_key(k)}: {v}" for k, v in expr.items())
            expr = f"{{{expr}}}"

        return expr.strip()

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

        language = self.properties["language"]
        expression_prop = self._prepare_expression(language, self.properties["expression"])

        # jmespath expression for map block must be enclosed in { }
        if not (expression_prop.startswith("{") and expression_prop.endswith("}")):
            raise ValueError("map expression must be in a json-like format enclosed in { }")

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
