import logging
from typing import Any, Dict, List, Optional, Tuple

import cassandra.auth
import cassandra.cluster
from cassandra.cluster import PreparedStatement
from datayoga_core import utils, write_utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context
from datayoga_core.result import Result

logger = logging.getLogger("dy")


class Block(DyBlock):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

        connection_name = self.properties.get("connection")
        connection_details = utils.get_connection_details(connection_name, context)
        if connection_details.get("type") != "cassandra":
            raise ValueError(f"{connection_name} is not a cassandra connection")

        auth_provider = cassandra.auth.PlainTextAuthProvider(
            username=connection_details.get("user"),
            password=connection_details.get("password"))

        cluster = cassandra.cluster.Cluster(
            connection_details.get("hosts"),
            port=connection_details.get("port", 9042),
            auth_provider=auth_provider)

        self.keyspace = self.properties.get("keyspace")

        logger.debug(f"Connecting to Cassandra")
        self.session = cluster.connect(self.keyspace)

        self.opcode_field = self.properties.get("opcode_field")
        self.table = self.properties.get("table")
        self.keys = self.properties.get("keys")
        self.mapping = self.properties.get("mapping")

        business_key_columns = [column["column"] for column in write_utils.get_column_mapping(self.keys)]
        mapping_columns = [column["column"] for column in write_utils.get_column_mapping(self.mapping)]

        pk_clause = f"{' and '.join([column + ' = ?' for column in business_key_columns])}"
        self.delete_stmt = f"delete from {self.keyspace}.{self.table} where {pk_clause}"

        self.upsert_stmt = f"update {self.keyspace}.{self.table} set {', '.join([column + ' = ?' for column in mapping_columns])} where {pk_clause}"

    async def run(self, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Result]]:
        logger.debug(f"Running {self.get_block_name()}")

        records_to_insert, records_to_update, records_to_delete = write_utils.group_records_by_opcode(data, self.opcode_field, self.keys)
        self.execute_upsert(records_to_insert + records_to_update)
        self.execute_delete(records_to_delete)

        return utils.produce_data_and_results(data)

    def get_future(self, stmt: PreparedStatement, record: Dict[str, Any]) -> Any:
        future = self.session.execute_async(stmt, record)
        future.add_errback(utils.reject_record, record)
        return future

    def execute_upsert(self, records: List[Dict[str, Any]]):
        if records:
            logger.debug(f"Upserting {len(records)} record(s) to {self.table} table")
            stmt = self.session.prepare(self.upsert_stmt)
            futures = []
            for record in records:
                record_to_upsert = write_utils.map_record(record, self.keys, self.mapping)
                futures.append(self.get_future(stmt, record_to_upsert))

            for future in futures:
                future.result()

    def execute_delete(self, records: List[Dict[str, Any]]):
        if records:
            logger.debug(f"Deleting {len(records)} record(s) from {self.table} table")
            stmt = self.session.prepare(self.delete_stmt)
            futures = []
            for record in records:
                record_to_delete = write_utils.map_record(record, self.keys)
                futures.append(self.get_future(stmt, record_to_delete))

            for future in futures:
                future.result()
