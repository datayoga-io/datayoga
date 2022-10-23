import logging
import os
from csv import DictReader
from typing import Any, Dict, List, Optional

from datayoga.producer import Producer as DyProducer
from datayoga.context import Context

logger = logging.getLogger("dy")


class Block(DyProducer):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        csv_file = self.properties["file"]

        if os.path.isabs(csv_file) or context is None:
            self.file = csv_file
        else:
            self.file = os.path.join(context.properties.get("data_path"), csv_file)

        logger.debug(f"file: {self.file}")
        self.batch_size = self.properties.get("batch_size", 1000)

    def produce(self) -> List[Dict[str, Any]]:
        logger.debug("Reading CSV")

        with open(self.file, 'r') as read_obj:
            records = list(DictReader(read_obj))

        for i, record in enumerate(records):
            yield {"msg_id": f"{i}", "value": record}
