import logging
from itertools import groupby
from typing import Any, Dict, List, Optional, Tuple

import cassandra.auth
import cassandra.cluster
from cassandra.cluster import BatchStatement
from datayoga_core import utils, write_utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context
from datayoga_core.opcode import OpCode
from datayoga_core.result import Result, Status

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

        data = write_utils.write_records(data, self.opcode_field, self.execute_upsert, self.execute_delete)
        return utils.produce_data_and_results(data)

    def execute_upsert(self, records: List[Dict[str, Any]]):
        records_to_upsert = write_utils.get_records_to_upsert(records, self.keys, self.mapping)

        logger.debug(f"Upserting {len(records_to_upsert)} record(s) to {self.table} table")
        if records_to_upsert:
            batch = BatchStatement()
            stmt = self.session.prepare(self.upsert_stmt)
            for record in records_to_upsert:
                batch.add(stmt.bind(record))
            self.session.execute(batch)

    def execute_delete(self, records: List[Dict[str, Any]]):
        records_to_delete = write_utils.get_records_to_delete(records, self.keys)

        logger.debug(f"Deleting {len(records_to_delete)} record(s) from {self.table} table")
        if records_to_delete:
            batch = BatchStatement()
            stmt = self.session.prepare(self.delete_stmt)
            for record in records_to_delete:
                batch.add(stmt.bind(record))
            self.session.execute(batch)
