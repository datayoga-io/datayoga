import json
import logging
import select
import sys
import uuid
from typing import Any, Dict, Generator, List, Optional

from datayoga_core.context import Context
from datayoga_core.producer import Message
from datayoga_core.producer import Producer as DyProducer

logger = logging.getLogger("dy")


class Block(DyProducer):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

    def produce(self) -> Generator[Message, None, None]:
        if select.select([sys.stdin, ], [], [], 0.0)[0]:
            # piped data exists
            for data in sys.stdin:
                for record in self.get_records(data):
                    yield self.get_message(record)
        else:
            # interactive mode
            print("Enter data to process:")
            data = input()
            for record in self.get_records(data):
                yield self.get_message(record)

    def get_records(self, data: str) -> List[Dict[str, Any]]:
        records = json.loads(data)

        if isinstance(records, dict):
            records = [records]

        return records

    def get_message(self, record: Dict[str, Any]) -> Message:
        return {self.MSG_ID_FIELD: str(uuid.uuid4()), **record}
