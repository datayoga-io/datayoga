import logging
from typing import Any, Dict

from datayoga.dy_block import Block

logger = logging.getLogger(__name__)


class RemoveField(Block):
    def init(self):
        logger.info("remove_field: init")

    def transform(self, data: Dict[str, Any]):
        logger.info("remove_field: transform")
