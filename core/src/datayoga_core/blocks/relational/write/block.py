import logging
from abc import ABCMeta
from contextlib import suppress
from typing import Any, Callable, Dict, List, Optional, Tuple

import sqlalchemy as sa
from datayoga_core import utils, write_utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.blocks.relational import utils as relational_utils
from datayoga_core.context import Context
from datayoga_core.opcode import OpCode
from datayoga_core.result import BlockResult, Result, Status
from sqlalchemy.exc import OperationalError

logger = logging.getLogger("dy")


class Block(DyBlock, metaclass=ABCMeta):
    _engine_fields = ("business_key_columns", "mapping_columns", "columns",
                      "delete_stmt", "upsert_stmt", "tbl", "connection", "engine")

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

        self.context = context
        self.engine = None
        self.setup_engine()

    def setup_engine(self):
        """Sets up the SQLAlchemy engine and configure it."""
        if self.engine:
            return

        self.engine, self.db_type = relational_utils.get_engine(self.properties["connection"], self.context)
        self.schema = self.properties.get("schema")
        self.table = self.properties.get("table")
        self.opcode_field = self.properties.get("opcode_field")
        self.load_strategy = self.properties.get("load_strategy")
        self.keys = self.properties.get("keys")
        self.mapping = self.properties.get("mapping")
        self.foreach = self.properties.get("foreach")
        self.tbl = sa.Table(self.table, sa.MetaData(schema=self.schema), autoload_with=self.engine)

        if self.opcode_field:
            self.business_key_columns = [column["column"] for column in write_utils.get_column_mapping(self.keys)]
            self.mapping_columns = [column["column"] for column in write_utils.get_column_mapping(self.mapping)]

            self.columns = self.business_key_columns + [x for x in self.mapping_columns
                                                        if x not in self.business_key_columns]

            for column in self.columns:
                if not any(col.name.lower() == column.lower() for col in self.tbl.columns):
                    raise ValueError(f"{column} column does not exist in {self.tbl.fullname} table")

            conditions = []
            for business_key_column in self.business_key_columns:
                for tbl_column in self.tbl.columns:
                    if tbl_column.name.lower() == business_key_column.lower():
                        conditions.append(tbl_column == sa.bindparam(business_key_column))
                        break

            self.delete_stmt = self.tbl.delete().where(sa.and_(*conditions))
            self.upsert_stmt = self.generate_upsert_stmt()

    async def run(self, data: List[Dict[str, Any]]) -> BlockResult:
        """Runs the block with provided data and return the result."""
        logger.debug(f"Running {self.get_block_name()}")
        processed_records: List[Result] = []
        rejected_records: List[Result] = []

        self.setup_engine()

        if self.opcode_field:
            # Reject records with unknown opcodes
            opcode_groups = write_utils.group_records_by_opcode(data, opcode_field=self.opcode_field)
            rejected_records.extend([
                Result(status=Status.REJECTED, payload=record, message=f"unknown opcode '{opcode}'")
                for opcode in set(opcode_groups.keys()) - {o.value for o in OpCode}
                for record in opcode_groups[opcode]
            ])

            records_to_upsert = opcode_groups[OpCode.CREATE] + opcode_groups[OpCode.UPDATE]
            records_to_delete = opcode_groups[OpCode.DELETE]

            if self.foreach:
                records_to_upsert = utils.explode_records(records_to_upsert, self.foreach)
                records_to_delete = utils.explode_records(records_to_delete, self.foreach)

            upsert_processed, upsert_rejected = self.process_records(records_to_upsert, self.execute_upsert)
            delete_processed, delete_rejected = self.process_records(records_to_delete, self.execute_delete)

            processed_records.extend(upsert_processed)
            processed_records.extend(delete_processed)
            rejected_records.extend(upsert_rejected)
            rejected_records.extend(delete_rejected)

            return BlockResult(processed=processed_records, rejected=rejected_records)
        else:
            logger.debug(f"Inserting {len(data)} record(s) to {self.table} table")
            self.execute(self.tbl.insert(), data)
            return utils.all_success(data)

    def generate_upsert_stmt(self) -> Any:
        """Generates an UPSERT statement based on the DB type"""
        if self.db_type == relational_utils.DbType.PSQL:
            from sqlalchemy.dialects.postgresql import insert

            insert_stmt = insert(self.tbl).values({col: "?" for col in self.columns})
            return insert_stmt.on_conflict_do_update(
                index_elements=self.business_key_columns,
                set_={col: getattr(insert_stmt.excluded, col) for col in self.columns})

        if self.db_type == relational_utils.DbType.MYSQL:
            return sa.sql.text("""
                    INSERT INTO %s (%s)
                    VALUES (%s)
                    ON DUPLICATE KEY UPDATE %s
                    """ % (
                relational_utils.construct_table_reference(self.tbl),
                ", ".join([f"`{column}`" for column in self.columns]),
                ", ".join([f"{sa.bindparam(column)}" for column in self.columns]),
                ", ".join([f"`{column}` = VALUES(`{column}`)" for column in self.columns])
            ))

        if self.db_type == relational_utils.DbType.SQLSERVER:
            return sa.sql.text("""
                    MERGE %s AS target
                    USING (VALUES (%s)) AS source (%s) ON (%s)
                    WHEN NOT MATCHED BY target THEN INSERT (%s) VALUES (%s)
                    WHEN MATCHED THEN UPDATE SET %s;
                    """ % (
                relational_utils.construct_table_reference(self.tbl, with_brackets=True),
                ", ".join([f"{sa.bindparam(column)}" for column in self.business_key_columns]),
                ", ".join([f"[{column}]" for column in self.business_key_columns]),
                " AND ".join([f"target.[{column}] = source.[{column}]" for column in self.business_key_columns]),
                ", ".join([f"[{column}]" for column in self.columns]),
                ", ".join([f"{sa.bindparam(column)}" for column in self.columns]),
                ", ".join([f"target.[{column}] = {sa.bindparam(column)}" for column in self.mapping_columns])
            ))

        if self.db_type == relational_utils.DbType.ORACLE:
            return sa.sql.text("""
                    MERGE INTO %s target
                    USING (SELECT 1 FROM DUAL) ON (%s)
                    WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)
                    WHEN MATCHED THEN UPDATE SET %s
                    """ % (
                relational_utils.construct_table_reference(self.tbl),
                " AND ".join([f"target.{column} = :{column}" for column in self.business_key_columns]),
                ", ".join([f"{column}" for column in self.columns]),
                ", ".join([f"{sa.bindparam(column)}" for column in self.columns]),
                ", ".join([f"target.{column} = {sa.bindparam(column)}" for column in self.mapping_columns])
            ))

        if self.db_type == relational_utils.DbType.DB2:
            return sa.sql.text("""
                MERGE INTO %s AS target
                USING (VALUES (%s)) AS source (%s)
                ON (%s)
                WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)
                WHEN MATCHED THEN UPDATE SET %s
                """ % (
                relational_utils.construct_table_reference(self.tbl),
                ", ".join([f"{sa.bindparam(column)}" for column in self.business_key_columns]),
                ", ".join([f"{column}" for column in self.business_key_columns]),
                " AND ".join([f"target.{column} = source.{column}" for column in self.business_key_columns]),
                ", ".join([f"{column}" for column in self.columns]),
                ", ".join([f"{sa.bindparam(column)}" for column in self.columns]),
                ", ".join([f"target.{column} = {sa.bindparam(column)}" for column in self.mapping_columns])
            ))

    def process_records(
        self,
        records: List[Dict[str, Any]],
        execute_method: Callable[[List[Dict[str, Any]]], None]
    ) -> Tuple[List[Result], List[Result]]:
        """Processes records using the given execute method.

        Args:
            records (List[Dict[str, Any]]): List of records to process.
            execute_method (Callable[[List[Dict[str, Any]]], None]) Method to execute records (e.g., execute_upsert or execute_delete).

        Returns:
            Tuple[List[Result], List[Result]]: Processed and rejected records.
        """
        processed_records: List[Result] = []
        rejected_records: List[Result] = []

        try:
            execute_method(records)
            processed_records.extend([Result(Status.SUCCESS, payload=record) for record in records])
        except Exception as batch_error:
            logger.warning(f"Batch operation failed: {batch_error} - operations will be retried individually")
            for record in records:
                try:
                    execute_method([record])
                    processed_records.append(Result(Status.SUCCESS, payload=record))
                except Exception as individual_error:
                    rejected_records.append(
                        Result(
                            status=Status.REJECTED,
                            payload=record,
                            message=str(individual_error)
                        )
                    )

        return processed_records, rejected_records

    def execute(self, statement: Any, records: List[Dict[str, Any]]):
        """Executes a SQL statement with given records."""
        if isinstance(statement, str):
            statement = sa.sql.text(statement)

        logger.debug(f"Executing {statement} on {records}")
        connected = False
        try:
            with self.engine.connect() as connection:
                connected = True
                try:
                    connection.execute(statement, records)
                    if not connection._is_autocommit_isolation():
                        connection.commit()
                except OperationalError as e:
                    if self.db_type == relational_utils.DbType.MYSQL:
                        mysql_conn_errors = (
                            2002,  # CR_CONNECTION_ERROR
                            2003,  # CR_CONN_HOST_ERROR
                            2006,  # CR_SERVER_GONE_ERROR
                            2013,  # CR_SERVER_LOST
                            2055,  # CR_SERVER_LOST_EXTENDED
                        )

                        if e.orig.args[0] in mysql_conn_errors:
                            connected = False

                    raise
                except Exception:
                    raise
        except Exception as e:
            if not connected:
                raise ConnectionError(e) from e

            raise

    def execute_upsert(self, records: List[Dict[str, Any]]):
        """Upserts records into the table."""
        if records:
            logger.debug(f"Upserting {len(records)} record(s) to {self.table} table")
            records_to_upsert = []
            for record in records:
                records_to_upsert.append(write_utils.map_record(record, self.keys, self.mapping))

            if records_to_upsert:
                self.execute(self.upsert_stmt, records_to_upsert)

    def execute_delete(self, records: List[Dict[str, Any]]):
        """Deletes records from the table."""
        if records:
            logger.debug(f"Deleting {len(records)} record(s) from {self.table} table")
            records_to_delete = []
            for record in records:
                records_to_delete.append(write_utils.map_record(record, self.keys))

            if records_to_delete:
                self.execute(self.delete_stmt, records_to_delete)

    def stop(self):
        """Disposes of the engine and cleans up resources."""
        with suppress(Exception):
            if self.engine:
                self.engine.dispose()

        for attr in self._engine_fields:
            setattr(self, attr, None)
