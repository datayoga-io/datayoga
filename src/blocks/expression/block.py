import logging
from typing import Any, Dict, List

import jmespath
from datayoga.block import Block

logger = logging.getLogger(__name__)


class Expression(Block):
    def init(self):
        logger.info("expression: init")
        self.expression = jmespath.compile(self.properties["expression"])

    def run(self, data: List[Dict[str, Any]], context: Any = None) -> List[Dict[str, Any]]:
        logger.info("expression: run")

        for record in data:
            record[self.properties["field"]] = self.expression.search(record)

        return data
