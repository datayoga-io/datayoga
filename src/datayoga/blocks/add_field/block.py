import logging
from typing import Any

import jmespath
from datayoga.block import Block as DyBlock
from datayoga.blocks.enums import Language
from datayoga.blocks.utils import exec_sql, get_connection
from datayoga.context import Context

logger = logging.getLogger(__name__)


class Block(DyBlock):
    def init(self):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.language = self.properties["language"]

        if self.language == Language.JMESPATH.value:
            self.expression = jmespath.compile(self.properties["expression"])
        elif self.language == Language.SQL.value:
            self.conn = get_connection()

    def run(self, data: Any, context: Context = None) -> Any:
        logger.debug(f"Running {self.get_block_name()}")

        if self.language == Language.JMESPATH.value:
            data[self.properties["field"]] = self.expression.search(data)
        elif self.language == Language.SQL.value:
            data[self.properties["field"]] = exec_sql(self.conn, data.items(), self.properties["expression"])

        return data
