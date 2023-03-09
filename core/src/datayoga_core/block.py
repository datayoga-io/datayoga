from __future__ import annotations

import logging
from abc import ABCMeta
from typing import Any, Dict, List

from datayoga_core.entity import Entity
from datayoga_core.result import BlockResult

logger = logging.getLogger("dy")


class Block(Entity, metaclass=ABCMeta):

    async def run(self, data: List[Dict[str, Any]]) -> BlockResult:
        """ Transforms data

        Args:
            data (List[Dict[str, Any]]): Data

        Returns:
            BlockResult: Block result
        """
        raise NotImplementedError

    def stop(self):
        """
        Cleans the block connections and state
        """
        pass
