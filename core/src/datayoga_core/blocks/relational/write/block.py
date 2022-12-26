import logging
from enum import Enum, unique
from typing import Any, Dict, List, Optional, Tuple

import sqlalchemy as sa
from datayoga_core import utils, write_utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context
from datayoga_core.result import Result
from sqlalchemy import Table
from sqlalchemy.sql.expression import ColumnCollection

logger = logging.getLogger("dy")


@unique
class DbType(Enum):
    MYSQL = "mysql"
    PSQL = "postgresql"


def get_driver_name(db_type: str) -> str:
    if db_type == DbType.MYSQL.value:
        return "mysql+pymysql"

    return db_type


def generate_upsert_stmt(table: Table, business_key_columns: List[str], columns: List[str], db_type: str) -> Any:
    if db_type == DbType.PSQL.value:
        from sqlalchemy.dialects.postgresql import insert

        insert_stmt = insert(table).values({col: "?" for col in columns})
        return insert_stmt.on_conflict_do_update(
            index_elements=[table.columns[column] for column in business_key_columns],
            set_={col: getattr(insert_stmt.excluded, col) for col in columns})
    elif db_type == DbType.MYSQL.value:
        from sqlalchemy.dialects.mysql import insert

        insert_stmt = insert(table).values({col: "?" for col in columns})
        return insert_stmt.on_duplicate_key_update(
            ColumnCollection(columns=[(x.name, x) for x in [insert_stmt.inserted[column] for column in columns]]))

    raise ValueError(f"upsert for {db_type} is not supported yet")


class Block(DyBlock):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

        connection = utils.get_connection_details(self.properties.get("connection"), context)
        db_type = connection.get("type").lower()
        engine_url = sa.engine.URL.create(
            drivername=get_driver_name(db_type),
            host=connection.get("host"),
            port=connection.get("port"),
            username=connection.get("user"),
            password=connection.get("password"),
            database=connection.get("database")
        )

        self.schema = self.properties.get("schema")
        self.table = self.properties.get("table")
        self.opcode_field = self.properties.get("opcode_field")
        self.load_strategy = self.properties.get("load_strategy")
        self.keys = self.properties.get("keys")
        self.mapping = self.properties.get("mapping")

        self.engine = sa.create_engine(engine_url, echo=False)
        self.tbl = sa.Table(self.table, sa.MetaData(schema=self.schema), autoload_with=self.engine)

        if self.opcode_field:
            business_key_columns = [column["column"] for column in write_utils.get_column_mapping(self.keys)]
            mapping_columns = [column["column"] for column in write_utils.get_column_mapping(self.mapping)]

            columns = business_key_columns + [x for x in mapping_columns if x not in business_key_columns]

            for column in columns:
                if not column in self.tbl.columns:
                    raise ValueError(f"{column} column does not exist in {self.tbl.fullname} table")

            self.delete_stmt = self.tbl.delete().where(
                sa.and_(*[(self.tbl.columns[column] == sa.bindparam(column)) for column in business_key_columns]))

            self.upsert_stmt = generate_upsert_stmt(self.tbl, business_key_columns, columns, db_type)

        logger.debug(f"Connecting to {db_type}")
        self.conn = self.engine.connect()

    async def run(self, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Result]]:
        logger.debug(f"Running {self.get_block_name()}")

        if self.opcode_field:
            records_to_insert, records_to_update, records_to_delete = write_utils.group_records_by_opcode(
                data, self.opcode_field, self.keys)

            self.execute_upsert(records_to_insert + records_to_update)
            self.execute_delete(records_to_delete)
        else:
            logger.debug(f"Inserting {len(data)} record(s) to {self.table} table")
            self.conn.execute(self.tbl.insert(), data)

        return utils.produce_data_and_results(data)

    def execute_upsert(self, records: List[Dict[str, Any]]):
        if records:
            logger.debug(f"Upserting {len(records)} record(s) to {self.table} table")
            records_to_upsert = []
            for record in records:
                records_to_upsert.append(write_utils.map_record(record, self.keys, self.mapping))

            if records_to_upsert:
                self.conn.execute(self.upsert_stmt, records_to_upsert)

    def execute_delete(self, records: List[Dict[str, Any]]):
        if records:
            logger.debug(f"Deleting {len(records)} record(s) from {self.table} table")
            records_to_delete = []
            for record in records:
                records_to_delete.append(write_utils.map_record(record, self.keys))

            if records_to_delete:
                self.conn.execute(self.delete_stmt, records_to_delete)
