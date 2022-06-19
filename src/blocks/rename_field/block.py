import logging
from typing import Any

from datayoga.block import Block
from datayoga.context import Context

logger = logging.getLogger(__name__)


class RenameField(Block):
    def init(self):
        logger.info("rename_field: init")

    def run(self, data: Any, context: Context = None) -> Any:
        logger.info("rename_field: run")

        data[self.properties["to_field"]] = data.pop(self.properties["from_field"])
        return data
