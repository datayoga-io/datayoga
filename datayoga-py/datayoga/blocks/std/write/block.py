import logging
import sys
from typing import Any, Dict, List, Optional, Tuple

from datayoga.block import Block as DyBlock
from datayoga.block import Result
from datayoga.context import Context

logger = logging.getLogger("dy")


class Block(DyBlock):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

    async def run(self, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Result]]:
        for record in data:
            sys.stdout.write(f"{record}\n")

        # if we made it here, it is a success. return the data and the success result
        return data, [Result.SUCCESS]*len(data)
