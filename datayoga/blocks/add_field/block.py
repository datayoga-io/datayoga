import logging
from typing import Any, Dict, List

from datayoga import utils
from datayoga.block import Block as DyBlock
from datayoga.blocks import expression
from datayoga.context import Context

logger = logging.getLogger(__name__)


class Block(DyBlock):
    def init(self):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.properties = utils.format_block_properties(self.properties)

        for property in self.properties["fields"]:
            property["compiled_expression"] = expression.compile(property["language"], property["expression"])

    def run(self, data: List[Dict[str, Any]], context: Context = None) -> List[Dict[str, Any]]:
        logger.debug(f"Running {self.get_block_name()}")

        for row in data:
            for property in self.properties["fields"]:
                row[property["field"]] = property["compiled_expression"].search(row)

        return data
