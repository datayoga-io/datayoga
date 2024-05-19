import logging
from abc import ABCMeta
from contextlib import suppress
from typing import Any, Dict, List, Optional

import sqlalchemy as sa
from datayoga_core import utils, write_utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.blocks.relational import utils as relational_utils
from datayoga_core.context import Context
from datayoga_core.opcode import OpCode
from datayoga_core.result import BlockResult, Result, Status
from sqlalchemy import text
from sqlalchemy.exc import (DatabaseError, OperationalError,
                            PendingRollbackError)
from sqlalchemy.sql.expression import ColumnCollection

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
        if self.engine:
            return

        try:
            self.engine, self.db_type = relational_utils.get_engine(self.properties["connection"], self.context)

            logger.debug(f"Connecting to {self.db_type}")
            self.connection = self.engine.connect()

            # Disable the new MySQL 8.0.17+ default behavior of requiring an alias for ON DUPLICATE KEY UPDATE
            # This behavior is not supported by pymysql driver
            if self.engine.driver == "pymysql":
                self.engine.dialect._requires_alias_for_on_duplicate_key = False

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

        except OperationalError as e:
            self.dispose_engine()
            raise ConnectionError(e)
        except DatabaseError as e:
            # Handling specific OracleDB errors: Network failure and Database restart
            if self.db_type == relational_utils.DbType.ORACLE:
                self.handle_oracle_database_error(e)
            raise

    def dispose_engine(self):
        with suppress(Exception):
            self.connection.close()
        with suppress(Exception):
            self.engine.dispose()

        for attr in self._engine_fields:
            setattr(self, attr, None)

    async def run(self, data: List[Dict[str, Any]]) -> BlockResult:
        logger.debug(f"Running {self.get_block_name()}")
        rejected_records: List[Result] = []

        self.setup_engine()

        if self.opcode_field:
            opcode_groups = write_utils.group_records_by_opcode(data, opcode_field=self.opcode_field)
            # reject any records with unknown or missing Opcode
            for opcode in set(opcode_groups.keys()) - {o.value for o in OpCode}:
                rejected_records.extend([
                    Result(status=Status.REJECTED, payload=record, message=f"unknown opcode '{opcode}'")
                    for record in opcode_groups[opcode]
                ])

            records_to_upsert = opcode_groups[OpCode.CREATE] + opcode_groups[OpCode.UPDATE]
            records_to_delete = opcode_groups[OpCode.DELETE]
            if self.foreach:
                self.execute_upsert(utils.explode_records(records_to_upsert, self.foreach))
                self.execute_delete(utils.explode_records(records_to_delete, self.foreach))
            else:
                self.execute_upsert(records_to_upsert)
                self.execute_delete(records_to_delete)

            return BlockResult(
                processed=[Result(Status.SUCCESS, payload=record)
                           for opcode in OpCode for record in opcode_groups[opcode.value]],
                rejected=rejected_records)
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
            from sqlalchemy.dialects.mysql import insert

            insert_stmt = insert(self.tbl).values({col: "?" for col in self.columns})
            return insert_stmt.on_duplicate_key_update(ColumnCollection(
                columns=[(x.name, x) for x in [insert_stmt.inserted[column] for column in self.columns]]))

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

    def execute(self, statement: Any, records: List[Dict[str, Any]]):
        try:
            if isinstance(statement, str):
                statement = text(statement)
            logger.debug(f"Executing {statement} on {records}")
            self.connection.execute(statement, records)
            if not self.connection._is_autocommit_isolation():
                self.connection.commit()

        except (OperationalError, PendingRollbackError) as e:
            if self.db_type == relational_utils.DbType.SQLSERVER:
                self.handle_mssql_operational_error(e)

            self.dispose_engine()
            raise ConnectionError(e)
        except DatabaseError as e:
            if self.db_type == relational_utils.DbType.ORACLE:
                self.handle_oracle_database_error(e)

            raise

    def handle_mssql_operational_error(self, e):
        """Handling specific MSSQL cases: Conversion failed (245) and Truncated data (2628)"""
        if e.orig.args[0] in (245, 2628):
            raise

    def handle_oracle_database_error(self, e):
        """Handling specific OracleDB cases: Network failure (DPY-4011) and Database restart (ORA-01089)"""
        if "DPY-4011" in f"{e}" or "ORA-01089" in f"{e}":
            self.dispose_engine()
            raise ConnectionError(e)

    def execute_upsert(self, records: List[Dict[str, Any]]):
        if records:
            logger.debug(f"Upserting {len(records)} record(s) to {self.table} table")
            records_to_upsert = []
            for record in records:
                records_to_upsert.append(write_utils.map_record(record, self.keys, self.mapping))

            if records_to_upsert:
                self.execute(self.upsert_stmt, records_to_upsert)

    def execute_delete(self, records: List[Dict[str, Any]]):
        if records:
            logger.debug(f"Deleting {len(records)} record(s) from {self.table} table")
            records_to_delete = []
            for record in records:
                records_to_delete.append(write_utils.map_record(record, self.keys))

            if records_to_delete:
                self.execute(self.delete_stmt, records_to_delete)

    def stop(self):
        self.dispose_engine()
