import logging
import os
from csv import DictReader
from typing import Any, Dict, List, Optional

from datayoga.block import Block as DyBlock
from datayoga.context import Context

logger = logging.getLogger("dy")


class Block(DyBlock):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        csv_file = self.properties["file"]

        if os.path.isabs(csv_file) or context is None:
            self.file = csv_file
        else:
            self.file = os.path.join(context.properties.get("job_path"), csv_file)

        logger.debug(f"file: {self.file}")
        self.batch_size = self.properties.get("batch_size", 1000)

    def run(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        logger.debug("Reading CSV")

        with open(self.file, 'r') as read_obj:
            records = list(DictReader(read_obj))

        for i, record in enumerate(records):
            yield {"key": f"{i}", "value": record}
