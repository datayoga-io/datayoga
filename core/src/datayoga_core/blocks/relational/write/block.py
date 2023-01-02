import logging
from enum import Enum, unique
from typing import Any, Dict, List, Optional, Tuple

import sqlalchemy as sa
from datayoga_core import utils, write_utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context
from datayoga_core.result import Result
from sqlalchemy import Table
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql.expression import ColumnCollection

logger = logging.getLogger("dy")


@unique
class DbType(Enum):
    MSSQL = "mssql"
    MYSQL = "mysql"
    PSQL = "postgresql"

    @classmethod
    def has_value(cls, value: str) -> bool:
        return value in cls._value2member_map_


DEFAULT_DRIVERS = {
    DbType.MYSQL.value: "mysql+pymysql",
    DbType.MSSQL.value: "mssql+pymssql",
    DbType.PSQL.value: "postgresql"
}

class Block(DyBlock):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

        connection = utils.get_connection_details(self.properties.get("connection"), context)

        self.db_type = connection.get("type").lower()
        if not DbType.has_value(self.db_type):
            raise ValueError(f"{self.db_type} is not supported yet")

        self.schema = self.properties.get("schema")
        self.table = self.properties.get("table")
        self.opcode_field = self.properties.get("opcode_field")
        self.load_strategy = self.properties.get("load_strategy")
        self.keys = self.properties.get("keys")
        self.mapping = self.properties.get("mapping")

        engine = sa.create_engine(
            sa.engine.URL.create(
                drivername=connection.get("driver", DEFAULT_DRIVERS.get(self.db_type)),
                host=connection.get("host"),
                port=connection.get("port"),
                username=connection.get("user"),
                password=connection.get("password"),
                database=connection.get("database")),
            echo=connection.get("debug", False), connect_args=connection.get("connect_args", {}))
        self.tbl = sa.Table(self.table, sa.MetaData(schema=self.schema), autoload_with=engine)

        logger.debug(f"Connecting to {self.db_type}")
        self.connection = engine.connect()

        if self.db_type == DbType.MSSQL.value:
            # MERGE statement requires this
            self.connection = self.connection.execution_options(autocommit=True)

        if self.opcode_field:
            self.business_key_columns = [column["column"] for column in write_utils.get_column_mapping(self.keys)]
            self.mapping_columns = [column["column"] for column in write_utils.get_column_mapping(self.mapping)]

            self.columns = self.business_key_columns + [x for x in self.mapping_columns
                                                        if x not in self.business_key_columns]

            for column in self.columns:
                if not column in self.tbl.columns:
                    raise ValueError(f"{column} column does not exist in {self.tbl.fullname} table")

            self.delete_stmt = self.tbl.delete().where(
                sa.and_(*[(self.tbl.columns[column] == sa.bindparam(column)) for column in self.business_key_columns]))

            self.upsert_stmt = self.generate_upsert_stmt()

    async def run(self, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Result]]:
        logger.debug(f"Running {self.get_block_name()}")

        if self.opcode_field:
            records_to_insert, records_to_update, records_to_delete = write_utils.group_records_by_opcode(
                data, self.opcode_field, self.keys)

            self.execute_upsert(records_to_insert + records_to_update)
            self.execute_delete(records_to_delete)
        else:
            logger.debug(f"Inserting {len(data)} record(s) to {self.table} table")
            self.execute(self.tbl.insert(), data)

        return utils.produce_data_and_results(data)

    def generate_upsert_stmt(self) -> Any:
        """Generates an UPSERT statement based on the DB type"""
        if self.db_type == DbType.PSQL.value:
            from sqlalchemy.dialects.postgresql import insert

            insert_stmt = insert(self.tbl).values({col: "?" for col in self.columns})
            return insert_stmt.on_conflict_do_update(
                index_elements=self.business_key_columns,
                set_={col: getattr(insert_stmt.excluded, col) for col in self.columns})

        if self.db_type == DbType.MYSQL.value:
            from sqlalchemy.dialects.mysql import insert

            insert_stmt = insert(self.tbl).values({col: "?" for col in self.columns})
            return insert_stmt.on_duplicate_key_update(ColumnCollection(
                columns=[(x.name, x) for x in [insert_stmt.inserted[column] for column in self.columns]]))

        if self.db_type == DbType.MSSQL.value:
            return sa.sql.text("""
                    MERGE %s AS target
                    USING (VALUES (%s)) AS source (%s) ON (%s)
                    WHEN NOT MATCHED BY target THEN INSERT (%s) VALUES (%s)
                    WHEN MATCHED THEN UPDATE SET %s;
                    """ % (
                f"[{self.tbl.schema}].[{self.tbl.name}]",
                ", ".join([f"{sa.bindparam(column)}" for column in self.business_key_columns]),
                ", ".join([f"[{column}]" for column in self.business_key_columns]),
                "AND ".join([f"target.[{column}] = source.[{column}]" for column in self.business_key_columns]),
                ", ".join([f"[{column}]" for column in self.columns]),
                ", ".join([f"{sa.bindparam(column)}" for column in self.columns]),
                ", ".join([f"target.[{column}] = {sa.bindparam(column)}" for column in self.mapping_columns])
            ))

    def execute(self, statement: Any, records: List[Dict[str, Any]]) -> CursorResult:
        try:
            return self.connection.execute(statement, records)
        except OperationalError as e:
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
