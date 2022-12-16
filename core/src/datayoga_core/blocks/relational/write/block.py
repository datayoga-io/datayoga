import logging
from enum import Enum, unique
from itertools import groupby
from typing import Any, Dict, List, Optional, Tuple, Union

import sqlalchemy as sa
from datayoga_core import utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context
from datayoga_core.result import Result, Status
from sqlalchemy import Table
from sqlalchemy.dialects.postgresql import insert

logger = logging.getLogger("dy")


@unique
class OpCode(Enum):
    CREATE = "c"
    DELETE = "d"
    UPDATE = "u"


def get_source_column(item: Union[Dict[str, str], str]) -> str:
    return next(iter(item.values())) if isinstance(item, dict) else item


def get_target_column(item: Union[Dict[str, str], str]) -> str:
    return next(iter(item.keys())) if isinstance(item, dict) else item


def get_column_mapping(mapping: List[Union[Dict[str, str], str]]) -> List[Dict[str, str]]:
    return [{"column": next(iter(item.keys())), "key": next(iter(item.values()))} if isinstance(item, dict)
            else {"column": item, "key": item} for item in mapping] if mapping else []


def generate_upsert_stmt(table: Table, business_key_columns: List[str], columns: List[str], db_type: str) -> Any:
    if db_type.lower() == "postgresql":
        insert_stmt = insert(table).values(columns)
        return insert_stmt.on_conflict_do_update(
            index_elements=[table.columns[column] for column in business_key_columns],
            set_={col: getattr(insert_stmt.excluded, col) for col in columns})

    raise ValueError(f"upsert for {db_type} is not supported yet")


def get_key_values(record: Dict[str, Any], keys: List[Union[Dict[str, Any], str]]) -> Dict[str, Any]:
    key_values = {}
    for item in keys:
        source_key = get_source_column(item)
        if source_key not in record:
            logger.warning(f"{source_key} key does not exist in record:\n{record}")
            raise ValueError(f"{source_key} key does not exist")

        key_values[get_target_column(item)] = record[source_key]

    return key_values


class Block(DyBlock):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

        connection = utils.get_connection_details(self.properties.get("connection"), context)
        db_type = connection.get("type")
        engine_url = sa.engine.URL.create(
            drivername=db_type,
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
            business_key_columns = [column["column"] for column in get_column_mapping(self.keys)]
            mapping_columns = [column["column"] for column in get_column_mapping(self.mapping)]

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
            # group records by opcode
            opcodes = [{"opcode": key, "records": list(result)} for key, result in groupby(
                data, key=lambda record: record.get(self.opcode_field, "").replace(OpCode.CREATE.value, OpCode.UPDATE.value))]

            for records_by_opcode in opcodes:
                opcode = records_by_opcode["opcode"]
                records: List[Dict[str, Any]] = records_by_opcode["records"]

                logger.debug(f"Total {len(records)} record(s) with {opcode} opcode")
                if opcode == OpCode.UPDATE.value:
                    self.execute_upsert(records)
                elif opcode == OpCode.DELETE.value:
                    self.execute_delete(records)
                else:
                    for record in records:
                        record[Block.RESULT_FIELD] = Result(Status.REJECTED, f"{opcode} - unsupported opcode")
                    logger.warning(f"{opcode} - unsupported opcode")
        else:
            logger.debug(f"Inserting {len(data)} record(s) to {self.table} table")
            self.conn.execute(self.tbl.insert(), data)

        return utils.produce_data_and_results(data)

    def execute_upsert(self, records: List[Dict[str, Any]]):
        records_to_upsert = []
        for record in records:
            try:
                get_key_values(record, self.keys)
            except ValueError as e:
                record[Block.RESULT_FIELD] = Result(Status.REJECTED, f"{e}")

            # map the record to upsert based on the mapping definitions
            # add nulls for missing mapped fields
            record_to_upsert = {}
            for item in self.keys + self.mapping:
                source = get_source_column(item)
                target = get_target_column(item)
                record_to_upsert[target] = None if source not in record else record[source]

            records_to_upsert.append(record_to_upsert)

        logger.debug(f"Upserting {len(records_to_upsert)} record(s) to {self.table} table")
        if records_to_upsert:
            self.conn.execute(self.upsert_stmt, records_to_upsert)

    def execute_delete(self, records: List[Dict[str, Any]]):
        keys_to_delete = []
        for record in records:
            try:
                keys_to_delete.append(get_key_values(record, self.keys))
            except ValueError as e:
                record[Block.RESULT_FIELD] = Result(Status.REJECTED, f"{e}")

        logger.debug(f"Deleting {len(keys_to_delete)} record(s) from {self.table} table")
        if keys_to_delete:
            self.conn.execute(self.delete_stmt, keys_to_delete)
