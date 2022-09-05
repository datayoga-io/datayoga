import logging
from typing import Any, Dict, List

from datayoga.block import Block as DyBlock
from datayoga.context import Context

logger = logging.getLogger(__name__)


class Block(DyBlock):
    def init(self):
        logger.debug(f"Initializing {self.get_block_name()}")
        for property in self.properties:
            property["target_field"] = property.get("target_field", property.get("field"))
            property["delimiter"] = property.get("delimiter", ",")
            property["field"] = property["field"]

    def run(self, data: List[Dict[str, Any]], context: Context = None) -> List[Dict[str, Any]]:
        logger.debug(f"Running {self.get_block_name()}")
        return_data = []
        for row in data:
            for property in self.properties:
                # explode the field
                split_values = row[property["field"]].split(property["delimiter"])
                # add a list of the items with the new field
                return_data.extend([dict({property["target_field"]: value}, **row,) for value in split_values])

        return return_data
