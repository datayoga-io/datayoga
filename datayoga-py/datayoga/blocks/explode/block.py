import logging
from typing import Any, Dict, List

from datayoga.block import Block as DyBlock
from datayoga.context import Context

logger = logging.getLogger("dy")


class Block(DyBlock):
    def init(self, context: Context = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.target_field = self.properties.get("target_field", self.properties.get("field"))
        self.delimiter = self.properties.get("delimiter", ",")
        self.field = self.properties["field"]

    async def run(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        logger.debug(f"Running {self.get_block_name()}")
        return_data = []
        for row in data:
            # explode the field
            split_values = row[self.properties["field"]].split(self.properties["delimiter"])
            # add a list of the items with the new field
            return_data.extend([dict({self.target_field: value}, **row,) for value in split_values])

        return return_data
