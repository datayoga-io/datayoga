import logging
from enum import Enum, unique
from itertools import groupby
from typing import Any, Dict, List, Optional, Tuple, Union

import sqlalchemy as sa
from datayoga_core import utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context
from datayoga_core.result import Result, Status

logger = logging.getLogger("dy")


@unique
class OpCode(Enum):
    CREATE = "c"
    DELETE = "d"
    UPDATE = "u"


def get_field(item: Union[Dict[str, str], str]) -> str:
    return str(next(iter(item.values()))) if isinstance(item, dict) else item


def get_fields(mapping: Optional[Union[Dict[str, Any], str]]) -> List[Dict[str, Any]]:
    return [{"column": str(next(iter(item.keys()))),
             "key": str(next(iter(item.values())))}
            if isinstance(item, dict) else {"column": item, "key": item} for item in mapping] if mapping else []


def generate_upsert_stmt(
        table: str, primary_keys: List[Dict[str, Any]],
        mapping_fields: List[Dict[str, Any]],
        db_type: str) -> Any:
    if db_type.lower() == "postgresql":
        update_fields = [f"{field['column']} = {sa.bindparam(field['key'])}" for field in mapping_fields]

        insert_fields = ", ".join([field["column"] for field in mapping_fields])
        pk_fields = ", ".join([field["column"] for field in primary_keys])

        insert_bind_params = ", ".join([f"{sa.bindparam(field['key'])}" for field in mapping_fields])

        return sa.text(f"""INSERT INTO {table} ({insert_fields})
                           VALUES ({insert_bind_params})
                           ON CONFLICT({pk_fields}) DO UPDATE
                           SET {', '.join(update_fields)}""")

    raise ValueError(f"upsert for {db_type} is not supported yet")


def get_key_values(record: Dict[str, Any], keys: List[Union[Dict[str, Any], str]]) -> Dict[str, Any]:
    key_values = {}
    for item in keys:
        key = get_field(item)
        if key not in record:
            logger.warning(f"{key} key does not exist in record:\n{record}")
            raise ValueError(f"{key} key does not exist")

        key_values[key] = record[key]

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
            primary_keys = get_fields(self.keys)
            mapping_fields = get_fields(self.mapping)

            self.delete_stmt = self.tbl.delete(
                sa.and_(*[sa.text(f"{self.tbl.columns.get(field['column'])} = {sa.bindparam(field['key'])}")
                          for field in primary_keys]))

            self.upsert_stmt = generate_upsert_stmt(self.tbl.fullname, primary_keys, mapping_fields, db_type)

        logger.debug(f"Connecting to {connection.get('type')}")
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

            # add nulls for missing mapped fields
            for item in self.mapping:
                field = get_field(item)
                if field not in record:
                    record[field] = None

            records_to_upsert.append(record)

        logger.debug(f"Upserting {len(records_to_upsert)} record(s) to {self.table} table")
        if records_to_upsert:
            self.conn.execute(self.upsert_stmt, records_to_upsert)

    def execute_delete(self, records: List[Dict[str, Any]]):
        keys_to_delete = []
        for record in records:
            try:
                key_to_delete = get_key_values(record, self.keys)
                keys_to_delete.append(key_to_delete)
            except ValueError as e:
                record[Block.RESULT_FIELD] = Result(Status.REJECTED, f"{e}")

        logger.debug(f"Deleting {len(keys_to_delete)} record(s) from {self.table} table")
        if keys_to_delete:
            self.conn.execute(self.delete_stmt, keys_to_delete)
