import logging
from typing import Any, Dict, List, Optional

from datayoga_core import expression, utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context
from datayoga_core.result import BlockResult

logger = logging.getLogger("dy")


class Block(DyBlock):
    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.properties = utils.format_block_properties(self.properties)

        self.fields = {}
        for property in self.properties["fields"]:
            self.fields[property["field"]] = expression.compile(
                property["language"],
                property["expression"])

    async def run(self, data: List[Dict[str, Any]]) -> BlockResult:
        logger.debug(f"Running {self.get_block_name()}")
        for field in self.fields:
            expression_results = self.fields[field].search_bulk(data)
            field_path = utils.split_field(field)

            for i, row in enumerate(data):
                obj = row
                # handle nested fields. in that case, the obj points at the nested entity
                for key in field_path[:-1]:
                    key = utils.unescape_field(key)
                    if key in obj:
                        obj = obj[key]
                    else:
                        obj[key] = {}
                # assign the new values
                obj[utils.unescape_field(field_path[-1:][0])] = expression_results[i]

        return utils.all_success(data)
