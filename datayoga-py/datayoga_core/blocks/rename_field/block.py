import logging
from typing import Any, Dict, List, Tuple

from datayoga_core import utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.block import Result
from datayoga_core.context import Context

logger = logging.getLogger("dy")


class Block(DyBlock):
    def init(self, context: Context = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.properties = utils.format_block_properties(self.properties)

    async def run(self, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Result]]:
        logger.debug(f"Running {self.get_block_name()}")
        results = []

        for row in data:
            for property in self.properties["fields"]:
                key_found = True
                obj = row
                value = None
                from_field_path = utils.split_field(property["from_field"])

                for index, key in enumerate(from_field_path):
                    key = utils.unescape_field(key)

                    if key in obj:
                        if len(from_field_path) == index + 1:
                            value = obj[key]
                            del obj[key]
                        else:
                            obj = obj[key]
                    else:
                        key_found = False
                        break

                if key_found:
                    obj = row
                    to_field_path = utils.split_field(property["to_field"])

                    for key in to_field_path[:-1]:
                        key = utils.unescape_field(key)

                        if key in obj:
                            obj = obj[key]
                        else:
                            obj[key] = {}

                    if key in obj:
                        obj = obj[key]

                    obj[utils.unescape_field(to_field_path[-1:][0])] = value

        results.append(Result.SUCCESS)
        return data, results
