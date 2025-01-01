import datetime
import logging
from typing import (Any, AsyncGenerator, Dict, Generator, Iterator, List,
                    Optional)

import datayoga_core.blocks.redis.utils as redis_utils
import orjson
import snowflake.connector
from datayoga_core.connection import Connection
from datayoga_core.context import Context
from datayoga_core.producer import Message
from datayoga_core.producer import Producer as DyProducer
from datayoga_core.result import JobResult

logger = logging.getLogger(__name__)


class Block(DyProducer):
    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

        self.connection_details = Connection.get_connection_details(self.properties["connection"], context)
        self.snowflake_client = snowflake.connector.connect(**self.connection_details)

        self.stream_name = self.properties["stream_name"]
        self.stage_table_name = "_dy_cdc_" + self.stream_name
        self.snapshot = self.properties.get("snapshot", False)
        self.batch_size = 1000
        self._setup_stage_table()

    def produce(self) -> Generator[List[Message], JobResult, None]:

        logger.debug(f"Running {self.get_block_name()}")
        cursor = self.snowflake_client.cursor()
        cursor.execute(f"""
            SELECT
                OBJECT_CONSTRUCT(* exclude (metadata$action,metadata$row_id,metadata$isupdate)) as STREAM_DATA,
                METADATA$ACTION,
                METADATA$ISUPDATE,
                METADATA$ROW_ID
            FROM {self.stream_name}
        """)

        while True:
            cursor = self.snowflake_client.cursor()
            cursor.execute(f"""
                SELECT
                    OBJECT_CONSTRUCT(* exclude (metadata$action,metadata$row_id,metadata$isupdate)) as STREAM_DATA,
                    METADATA$ACTION,
                    METADATA$ISUPDATE,
                    METADATA$ROW_ID
                FROM {self.stream_name}
            """)
            records = cursor.fetchmany(self.batch_size)
            while len(records) > 0:

                print(len(records))
                result = yield self._process_records(records)
                self.ack(result)
                records = cursor.fetchmany(self.batch_size)

    def ack(self, msg_ids: List[str]):
        cursor = self.snowflake_client.cursor()
        cursor.execute(f"""
            create or replace temp table _test_cdc_stream_flush as select * from test_cdc_stream where 0=1
        """)

    def _setup_stage_table(self):
        """Create the staging table if it doesn't exist"""
        try:
            # First, check if table exists and has the right structure
            self.snowflake_client.cursor().execute(f"""
            CREATE TABLE IF NOT EXISTS {self.stage_table_name} (
                CHANGE_ID NUMBER AUTOINCREMENT,
                STREAM_DATA OBJECT,
                METADATA$ACTION STRING,
                METADATA$ISUPDATE BOOLEAN,
                METADATA$ROW_ID STRING,
                LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """)
            logger.info(f"Ensured stage table {self.stage_table_name} exists")

        except Exception as e:
            logger.error(f"Error setting up stage table: {str(e)}")
            raise

    def _check_unprocessed_records(self) -> bool:
        """Check if there are any unprocessed records in the stage table"""
        cursor = self.snowflake_client.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {self.stage_table_name}")
        count = cursor.fetchone()[0]
        return count > 0

    def _process_records(self, records) -> List[Message]:
        results = []
        for record in records:
            str_data, action, is_update, row_id = record
            data = orjson.loads(str_data)

            # Convert to Debezium-like format
            operation = {
                'INSERT': 'c',
                'DELETE': 'd',
                'UPDATE': 'u'
            }.get(action, 'c')

            # For updates, we need before/after state
            before_value = None
            after_value = None

            if operation == 'c':  # Insert
                after_value = data
            elif operation == 'd':  # Delete
                before_value = data
            elif operation == 'u':  # Update
                before_value = data.get('_before') if isinstance(
                    data, dict) else None
                after_value = data.get('_after') if isinstance(
                    data, dict) else data

            debezium_format = {
                "payload": {
                    "before": before_value,
                    "after": after_value,
                    "source": {
                        "version": "1.0",
                        "connector": "snowflake",
                        "name": self.stream_name,
                        "ts_ms": datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000,
                        "snapshot": False,
                        "db": self.connection_details['database'],
                        "schema": self.connection_details['schema'],
                        "table": self.stream_name,
                        "txId": row_id,
                    },
                    "op": operation,
                    "ts_ms": int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000),
                    "transaction": None
                }
            }
            print(debezium_format)
            results.append(Message(row_id, debezium_format))
        return results
