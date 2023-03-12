import logging
from abc import ABCMeta
from typing import Any, Dict, List, Optional

from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context

logger = logging.getLogger(__name__)


class Block(DyBlock, metaclass=ABCMeta):

    def init(self, context: Optional[Context] = None):
        pass

    def run(self, data: List[Dict[str, Any]], context: Context = None) -> List[Dict[str, Any]]:
        pass
