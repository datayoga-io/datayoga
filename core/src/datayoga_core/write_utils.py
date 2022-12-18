import logging
from itertools import groupby
from typing import Any, Callable, Dict, List, Union

from datayoga_core.block import Block
from datayoga_core.opcode import OpCode
from datayoga_core.result import Result, Status

logger = logging.getLogger("dy")


def write_records(data: List[Dict[str, Any]],
                  opcode_field: str, execute_upsert: Callable, execute_delete: Callable) -> List[Dict[str, Any]]:
    # group records by opcode
    opcodes = [{"opcode": key, "records": list(result)} for key, result in groupby(
        data, key=lambda record: record.get(opcode_field, "").replace(OpCode.CREATE.value, OpCode.UPDATE.value))]

    for records_by_opcode in opcodes:
        opcode = records_by_opcode["opcode"]
        records: List[Dict[str, Any]] = records_by_opcode["records"]

        logger.debug(f"Total {len(records)} record(s) with {opcode} opcode")
        if opcode == OpCode.UPDATE.value:
            execute_upsert(records)
        elif opcode == OpCode.DELETE.value:
            execute_delete(records)
        else:
            for record in records:
                record[Block.RESULT_FIELD] = Result(Status.REJECTED, f"{opcode} - unsupported opcode")
            logger.warning(f"{opcode} - unsupported opcode")

    return data


def get_source_column(item: Union[Dict[str, str], str]) -> str:
    return next(iter(item.values())) if isinstance(item, dict) else item


def get_target_column(item: Union[Dict[str, str], str]) -> str:
    return next(iter(item.keys())) if isinstance(item, dict) else item


def get_column_mapping(mapping: List[Union[Dict[str, str], str]]) -> List[Dict[str, str]]:
    return [{"column": next(iter(item.keys())), "key": next(iter(item.values()))} if isinstance(item, dict)
            else {"column": item, "key": item} for item in mapping] if mapping else []


def get_key_values(record: Dict[str, Any], keys: List[Union[Dict[str, Any], str]]) -> Dict[str, Any]:
    key_values = {}
    for item in keys:
        source_key = get_source_column(item)
        if source_key not in record:
            raise ValueError(f"{source_key} key does not exist")

        key_values[get_target_column(item)] = record[source_key]

    return key_values


def get_records_to_upsert(records: List[Dict[str, Any]],
                          keys: List[Union[Dict[str, Any], str]],
                          mapping: List[Union[Dict[str, Any], str]]) -> List[Dict[str, Any]]:
    records_to_upsert = []
    for record in records:
        try:
            get_key_values(record, keys)
        except ValueError as e:
            record[Block.RESULT_FIELD] = Result(Status.REJECTED, f"{e}")

        # map the record to upsert based on the mapping definitions
        # add nulls for missing mapped fields
        record_to_upsert = {}
        for item in keys + mapping:
            source = get_source_column(item)
            target = get_target_column(item)
            record_to_upsert[target] = None if source not in record else record[source]

        records_to_upsert.append(record_to_upsert)

    return records_to_upsert


def get_records_to_delete(records: List[Dict[str, Any]],
                          keys: List[Union[Dict[str, Any], str]]) -> List[Dict[str, Any]]:
    records_to_delete = []
    for record in records:
        try:
            records_to_delete.append(get_key_values(record, keys))
        except ValueError as e:
            record[Block.RESULT_FIELD] = Result(Status.REJECTED, f"{e}")

    return records_to_delete
