import logging
from typing import Any, Dict

from datayoga.dy_block import Block

logger = logging.getLogger(__name__)


class RenameField(Block):
    def init(self):
        logger.info("rename_field: init")

    def transform(self, data: Dict[str, Any]):
        logger.info("rename_field: transform")
