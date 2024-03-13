import logging
from abc import ABCMeta
from typing import Any, Dict, List, Optional

import cassandra.auth
from cassandra.cluster import NoHostAvailable, PreparedStatement
from datayoga_core import write_utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.connection import Connection
from datayoga_core.context import Context
from datayoga_core.opcode import OpCode
from datayoga_core.result import BlockResult, Result, Status

logger = logging.getLogger("dy")


class Block(DyBlock, metaclass=ABCMeta):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

        connection_name = self.properties["connection"]
        connection_details = Connection.get_connection_details(connection_name, context)
        if connection_details.get("type") != "cassandra":
            raise ValueError(f"{connection_name} is not a cassandra connection")

        auth_provider = cassandra.auth.PlainTextAuthProvider(
            username=connection_details.get("user"),
            password=connection_details.get("password"))

        self.cluster = cassandra.cluster.Cluster(
            connection_details.get("hosts"),
            port=connection_details.get("port", 9042),
            auth_provider=auth_provider)

        self.keyspace = self.properties.get("keyspace")

        logger.debug(f"Connecting to Cassandra")
        self.session = self.cluster.connect(self.keyspace)

        self.opcode_field = self.properties.get("opcode_field")
        self.table = self.properties.get("table")
        self.keys = self.properties.get("keys")
        self.mapping = self.properties.get("mapping")

        business_key_columns = [column["column"] for column in write_utils.get_column_mapping(self.keys)]
        mapping_columns = [column["column"] for column in write_utils.get_column_mapping(self.mapping)]

        pk_clause = f"{' and '.join([column + ' = ?' for column in business_key_columns])}"
        self.delete_stmt = f"delete from {self.keyspace}.{self.table} where {pk_clause}"

        self.upsert_stmt = f"update {self.keyspace}.{self.table} set {', '.join([column + ' = ?' for column in mapping_columns])} where {pk_clause}"

    async def run(self, data: List[Dict[str, Any]]) -> BlockResult:
        logger.debug(f"Running {self.get_block_name()}")

        opcode_groups = write_utils.group_records_by_opcode(data, opcode_field=self.opcode_field)
        # reject any records with unknown or missing Opcode
        rejected_records: List[Result] = []

        for opcode in set(opcode_groups.keys()) - {o.value for o in OpCode}:
            rejected_records.extend([
                Result(status=Status.REJECTED, payload=record, message=f"unknown opcode '{opcode}'")
                for record in opcode_groups[opcode]
            ])

        try:
            self.execute_upsert(opcode_groups[OpCode.CREATE] + opcode_groups[OpCode.UPDATE])
            self.execute_delete(opcode_groups[OpCode.DELETE])
        except NoHostAvailable as e:
            raise ConnectionError(e)

        return BlockResult(
            processed=[Result(Status.SUCCESS, payload=record)
                       for opcode in OpCode for record in opcode_groups[opcode.value]],
            rejected=rejected_records)

    def get_future(self, stmt: PreparedStatement, record: Dict[str, Any]) -> Any:
        future = self.session.execute_async(stmt, record)
        future.add_errback(lambda ex, record: Result(status=Status.REJECTED, payload=record, message=f"{ex}"), record)
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

    def stop(self):
        self.cluster.shutdown()
