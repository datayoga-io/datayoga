import gzip
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
            self.filename = csv_file
        else:
            self.filename = os.path.join(context.properties.get("data_path"), csv_file)

        logger.debug(f"file: {self.filename}")
        self.batch_size = self.properties.get("batch_size", 1000)

    def run(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        logger.debug("Reading CSV")
        # test if a gzipped file
        infile = None
        try:
            if os.path.splitext(self.filename)[-1] == ".gz":
                infile = gzip.open(self.filename, 'rt')
            else:
                infile = open(self.file, 'r')

            records = list(DictReader(infile))

            for i, record in enumerate(records):
                yield {"key": f"{i}", "value": record}
        finally:
            infile.close()
