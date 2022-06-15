import logging
from typing import Any, Dict

from datayoga.dy_block import Block

logger = logging.getLogger(__name__)


class Expression(Block):
    def init(self):
        logger.info("expression: init")

    def transform(self, data: Dict[str, Any]):
        logger.info("expression: transform")
