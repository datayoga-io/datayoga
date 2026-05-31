import logging
import select
import sys
import uuid
from typing import Any, AsyncGenerator, Dict, List, Optional

import orjson
from datayoga_core.context import Context
from datayoga_core.producer import Producer as DyProducer

logger = logging.getLogger("dy")


class Block(DyProducer):
    """Producer block that reads JSON records from standard input."""

    def init(self, context: Optional[Context] = None):
        """Initializes the block."""
        logger.debug(f"Initializing {self.get_block_name()}")

    async def produce_chunks(self) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """Reads all stdin records and yields them as a single chunk.

        The base class re-chunks the output to `batch_size` records per batch.
        """
        if select.select([sys.stdin], [], [], 0.0)[0]:
            all_records: List[Dict[str, Any]] = []
            for line in sys.stdin:
                all_records.extend(self.get_records(line))
        else:
            print("Enter data to process:")
            all_records = self.get_records(input())

        if all_records:
            yield [self.get_message(record) for record in all_records]

    @staticmethod
    def get_records(data: str) -> List[Dict[str, Any]]:
        """Parses a JSON string into a list of records (wraps single objects in a list)."""
        records = orjson.loads(data)
        if isinstance(records, dict):
            records = [records]
        return records

    def get_message(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Returns the record with a generated message id field added."""
        return {self.MSG_ID_FIELD: str(uuid.uuid4()), **record}
