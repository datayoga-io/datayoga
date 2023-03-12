import logging
from abc import ABCMeta
from typing import Any, Dict, List, Optional

from datayoga_core import utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context
from datayoga_core.result import BlockResult

logger = logging.getLogger("dy")


class Block(DyBlock, metaclass=ABCMeta):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.properties = utils.format_block_properties(self.properties)

    async def run(self, data: List[Dict[str, Any]]) -> BlockResult:
        logger.debug(f"Running {self.get_block_name()}")
        for row in data:
            for prop in self.properties["fields"]:
                key_found = True
                obj = row
                value = None
                from_field_path = utils.split_field(prop["from_field"])

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
                    to_field_path = utils.split_field(prop["to_field"])

                    for key in to_field_path[:-1]:
                        key = utils.unescape_field(key)

                        if key in obj:
                            obj = obj[key]
                        else:
                            obj[key] = {}

                    if key in obj:
                        obj = obj[key]

                    obj[utils.unescape_field(to_field_path[-1:][0])] = value

        return utils.all_success(data)
