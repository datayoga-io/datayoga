import logging
from typing import Any, Dict, List

from datayoga_core import utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.block import Result
from datayoga_core.context import Context

logger = logging.getLogger("dy")


class Block(DyBlock):
    def init(self, context: Context = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.properties = utils.format_block_properties(self.properties)

    async def run(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        logger.debug(f"Running {self.get_block_name()}")
        results = []

        for row in data:
            for property in self.properties["fields"]:
                obj = row
                from_field_path = utils.split_field(property["field"])

                for index, key in enumerate(from_field_path):
                    key = utils.unescape_field(key)
                    if key in obj:
                        if len(from_field_path) == index + 1:
                            del obj[key]
                        else:
                            obj = obj[key]
        results.append(Result.SUCCESS)

        return data, results
