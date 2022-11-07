import logging
from typing import Any, Dict, List

from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context

logger = logging.getLogger(__name__)


class Block(DyBlock):
    def init(self):
        pass

    def run(self, data: List[Dict[str, Any]], context: Context = None) -> List[Dict[str, Any]]:
        pass
