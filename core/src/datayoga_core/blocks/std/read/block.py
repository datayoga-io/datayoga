import logging
import select
import sys
import uuid
from typing import Any, AsyncGenerator, Dict, List, Optional

import orjson
from datayoga_core.context import Context
from datayoga_core.producer import Message
from datayoga_core.producer import Producer as DyProducer

logger = logging.getLogger("dy")


class Block(DyProducer):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.batch_size = int(self.properties.get("batch_size", 1000))
        logger.info(f"Using batch size: {self.batch_size}")

    async def process_batch(self, records: List[Dict[str, Any]]) -> AsyncGenerator[List[Message], None]:
        """Process records and yield batches according to batch_size"""
        batch = []
        for record in records:
            batch.append(self.get_message(record))

            # When batch is full, yield it
            if len(batch) >= self.batch_size:
                logger.info(f"Yielding batch of {len(batch)} records")
                yield batch
                batch = []

        # Yield any remaining records
        if batch:
            logger.info(f"Yielding final batch of {len(batch)} records")
            yield batch

    async def produce(self) -> AsyncGenerator[List[Message], None]:
        if select.select([sys.stdin, ], [], [], 0.0)[0]:
            # piped data exists
            all_records = []
            for data in sys.stdin:
                all_records.extend(self.get_records(data))
        else:
            # interactive mode
            print("Enter data to process:")
            data = input()
            all_records = self.get_records(data)

        async for batch in self.process_batch(all_records):
            yield batch

    @staticmethod
    def get_records(data: str) -> List[Dict[str, Any]]:
        records = orjson.loads(data)

        if isinstance(records, dict):
            records = [records]

        return records

    def get_message(self, record: Dict[str, Any]) -> Message:
        return {self.MSG_ID_FIELD: str(uuid.uuid4()), **record}
