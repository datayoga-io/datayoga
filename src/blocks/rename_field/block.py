import logging
from typing import Any, Dict, List

from datayoga.block import Block

logger = logging.getLogger(__name__)


class RenameField(Block):
    def init(self):
        logger.info("rename_field: init")

    def run(self, data: List[Dict[str, Any]], context: Any = None) -> List[Dict[str, Any]]:
        logger.info("rename_field: run")

        for record in data:
            record[self.properties["to_field"]] = record.pop(self.properties["from_field"])

        return data
