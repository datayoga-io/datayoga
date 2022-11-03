import ast
import logging
import select
import sys
import uuid
from typing import Any, Dict, Generator, List, Optional

import aioconsole
from datayoga.context import Context
from datayoga.producer import Message
from datayoga.producer import Producer as DyProducer

logger = logging.getLogger("dy")


class Block(DyProducer):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

    async def produce(self) -> Generator[Message, None, None]:
        if select.select([sys.stdin, ], [], [], 0.0)[0]:
            # piped data exists
            for data in sys.stdin:
                for record in self.get_records(data):
                    yield self.get_message(record)
        else:
            # interactive mode
            print("Enter data to process after each execution:")
            while True:
                data = await aioconsole.ainput()
                for record in self.get_records(data):
                    yield self.get_message(record)

    def get_records(self, data: str) -> List[Dict[str, Any]]:
        records = ast.literal_eval(data)

        if isinstance(records, dict):
            records = [records]

        return records

    def get_message(self, record: Dict[str, Any]) -> Message:
        return {self.MSG_ID_FIELD: str(uuid.uuid4()), **record}
