import logging
import sqlite3
from enum import Enum, unique
from typing import Any

import jmespath
from datayoga.block import Block
from datayoga.context import Context

logger = logging.getLogger(__name__)


@unique
class Language(Enum):
    JMESPATH = "jmespath"
    SQL = "sql"


class Expression(Block):
    def init(self):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.language = self.properties.get("language", Language.JMESPATH.value)
        if self.language == Language.JMESPATH.value:
            self.expression = jmespath.compile(self.properties["expression"])
        elif self.language == Language.SQL.value:
            self.conn = sqlite3.connect(":memory")

    def run(self, data: Any, context: Context = None) -> Any:
        logger.debug(f"Running {self.get_block_name()}")

        if self.language == Language.JMESPATH.value:
            data[self.properties["field"]] = self.expression.search(data)
        elif self.language == Language.SQL.value:
            clauses = []
            for k, v in data.items():
                clauses.append(f"'{v}' as '{k}'")

            from_clause = f"select {','.join(clauses)}"

            value = self.conn.execute(f"select {self.properties['expression']} from ({from_clause})").fetchone()[0]
            data[self.properties["field"]] = value

        return data
