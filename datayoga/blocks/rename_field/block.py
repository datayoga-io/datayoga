import logging
from typing import Any, Dict, List

from datayoga import utils
from datayoga.block import Block as DyBlock
from datayoga.context import Context

logger = logging.getLogger(__name__)


class Block(DyBlock):
    def init(self):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.properties = utils.format_block_properties(self.properties)

    def run(self, data: List[Dict[str, Any]], context: Context = None) -> List[Dict[str, Any]]:
        logger.debug(f"Running {self.get_block_name()}")

        for row in data:
            for property in self.properties["fields"]:
                key_found = True
                obj = row
                value = None
                from_field_path = property["from_field"].split(".")

                for index, key in enumerate(from_field_path):
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
                    to_field_path = property["to_field"].split(".")

                    for key in to_field_path[:-1]:
                        if key in obj:
                            obj = obj[key]
                        else:
                            obj[key] = {}

                    if key in obj:
                        obj = obj[key]

                    obj[to_field_path[-1:][0]] = value

        return data
