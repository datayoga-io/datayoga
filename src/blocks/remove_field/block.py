import logging
from typing import Any, Dict, List

from datayoga.block import Block

logger = logging.getLogger(__name__)


class RemoveField(Block):
    def init(self):
        logger.info("remove_field: init")

    def run(self, data: List[Dict[str, Any]], context: Any = None) -> List[Dict[str, Any]]:
        logger.info("remove_field: run")

        for record in data:
            del record[self.properties["field"]]

        return data
