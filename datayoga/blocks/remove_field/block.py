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
                obj = row
                from_field_path = utils.split_field(property["field"])

                for index, key in enumerate(from_field_path):
                    key = utils.unescape_field(key)
                    if key in obj:
                        if len(from_field_path) == index + 1:
                            del obj[key]
                        else:
                            obj = obj[key]

        return data
