import logging
import sqlite3
from typing import Any

import jmespath
from datayoga.block import Block

logger = logging.getLogger(__name__)


class Expression(Block):
    def init(self):
        logger.info("expression: init")
        self.expression = jmespath.compile(self.properties["expression"])

    def run(self, data: Any, context: Any = None) -> Any:
        logger.info("expression: run")

        data[self.properties["field"]] = self.expression.search(data)
        return data


def start(record, properties):
    conn = sqlite3.connect(":memory")
    clauses = []
    for k, v in record.items():
        clauses.append(f"'{v}' as '{k}'")

    from_clause = f"select {','.join(clauses)}"
    for column_definition in properties.get("columns"):
        # return f"select {list(column_definition.values())[0]} from ({from_clause})"
        value = conn.execute(f"select {list(column_definition.values())[0]} from ({from_clause})").fetchone()[0]
        record[list(column_definition.keys())[0]] = value
    return record
