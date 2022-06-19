import logging
from typing import Any

from datayoga.block import Block

logger = logging.getLogger(__name__)


class RemoveField(Block):
    def init(self):
        logger.info("remove_field: init")

    def run(self, data: Any, context: Any = None) -> Any:
        logger.info("remove_field: run")

        del data[self.properties["field"]]
        return data
