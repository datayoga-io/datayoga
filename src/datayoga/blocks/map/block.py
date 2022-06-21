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
            self.expressions = {}
            for field in self.properties["object"]:
                self.expressions[field] = jmespath.compile(self.properties["object"][field])
        elif self.language == Language.SQL.value:
            self.conn = get_connection()

    def run(self, data: Any, context: Context = None) -> Any:
        logger.debug(f"Running {self.get_block_name()}")

        new_data = {}
        for field in self.properties["object"]:
            if self.language == Language.JMESPATH.value:
                new_data[field] = self.expressions[field].search(data)
            elif self.language == Language.SQL.value:
                new_data[field] = exec_sql(self.conn, data.items(), self.properties["object"][field])

        return new_data
