import logging
from abc import ABCMeta
from typing import Any, Dict, List, Optional

from datayoga_core import expression, utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context
from datayoga_core.result import BlockResult

logger = logging.getLogger("dy")


class Block(DyBlock, metaclass=ABCMeta):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.properties = utils.format_block_properties(self.properties)

        self.fields = {}
        for prop in self.properties["fields"]:
            self.fields[prop["field"]] = expression.compile(prop["language"], prop["expression"])

    async def run(self, data: List[Dict[str, Any]]) -> BlockResult:
        logger.debug(f"Running {self.get_block_name()}")
        for field, expr in self.fields.items():
            expression_results = expr.search_bulk(data)

            for i, row in enumerate(data):
                utils.set_field(row, field, expression_results[i])

        return utils.all_success(data)
