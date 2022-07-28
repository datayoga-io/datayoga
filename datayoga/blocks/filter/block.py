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
        self.expression = expression.compile(self.language, self.properties["expression"])

    def run(self, data: Any, context: Context = None) -> Any:
        logger.debug(f"Running {self.get_block_name()}")
        if self.language == expression.Language.SQL.value:
            return self.expression.filter(data)
        else:
            return self.expression.test(data[0])
