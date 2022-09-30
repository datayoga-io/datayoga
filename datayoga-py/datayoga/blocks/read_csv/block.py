import logging
from csv import DictReader
from typing import Any, Dict, List, Optional

from datayoga.block import Block as DyBlock
from datayoga.context import Context

logger = logging.getLogger("dy")


class Block(DyBlock):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

        self.file = self.properties["file"]

    def run(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        logger.debug(f"Reading CSV")

        with open(self.file, 'r') as read_obj:
            records = list(DictReader(read_obj))

        for i, record in enumerate(records):
            yield {"key": f"{i}", "value": record}
