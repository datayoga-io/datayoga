import logging
from typing import Any

from datayoga.block import Block as DyBlock
from datayoga.blocks import expression
from datayoga.context import Context

logger = logging.getLogger(__name__)


class Block(DyBlock):
    def init(self):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.language = self.properties["language"]
        self.expression = expression.get_expression_class(self.language, self.properties["expression"])

    def run(self, data: Any, context: Context = None) -> Any:
        logger.debug(f"Running {self.get_block_name()}")
        if self.expression.test(data[0] if self.language == expression.Language.SQL.value else data):
            return data
        else:
            return []
