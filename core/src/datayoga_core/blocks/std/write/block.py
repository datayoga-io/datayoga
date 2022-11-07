import json
import logging
import sys
from typing import Any, Dict, List, Optional, Tuple

from datayoga_core.block import Block as DyBlock
from datayoga_core.block import Result
from datayoga_core.context import Context

logger = logging.getLogger("dy")


class Block(DyBlock):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

    async def run(self, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Result]]:
        for record in data:
            # remove the internal $$msg_id column
            filtered_record = {i: record[i] for i in record if i != Block.MSG_ID_FIELD}
            sys.stdout.write(f"{json.dumps(filtered_record)}\n")

        # if we made it here, it is a success. return the data and the success result
        return data, [Result.SUCCESS]*len(data)
