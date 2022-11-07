import logging
from typing import Any, Dict, List, Tuple

from datayoga_core import expression, utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.block import Result
from datayoga_core.context import Context

logger = logging.getLogger("dy")


class Block(DyBlock):
    def init(self, context: Context = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.properties = utils.format_block_properties(self.properties)

        self.fields = {}
        for property in self.properties["fields"]:
            self.fields[property["field"]] = expression.compile(
                property["language"],
                property["expression"])

    async def run(self, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Result]]:
        logger.debug(f"Running {self.get_block_name()}")
        results = []
        for row in data:
            for field in self.fields:
                obj = row
                field_path = utils.split_field(field)

                for key in field_path[:-1]:
                    key = utils.unescape_field(key)
                    if key in obj:
                        obj = obj[key]
                    else:
                        obj[key] = {}

                obj[utils.unescape_field(field_path[-1:][0])] = self.fields[field].search(row)
            results.append(Result.SUCCESS)
        return data, results
