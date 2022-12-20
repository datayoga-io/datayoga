import logging
from typing import Any, Dict, List, Tuple, Union

from datayoga_core import utils
from datayoga_core.opcode import OPCODES, OpCode

logger = logging.getLogger("dy")


def group_records_by_opcode(records: List[Dict[str, Any]],
                            opcode_field: str,
                            keys: List[Union[Dict[str, str], str]]) -> Tuple[List[Dict[str, Any]],
                                                                             List[Dict[str, Any]],
                                                                             List[Dict[str, Any]]]:
    # group records by their opcode, reject records with unsupported opcode or with missing keys
    records_to_update: List[Dict[str, Any]] = []
    records_to_delete: List[Dict[str, Any]] = []
    records_to_insert: List[Dict[str, Any]] = []

    for record in records:
        opcode = record.get(opcode_field)
        if opcode is None:
            utils.reject_record(f"{opcode_field} opcode field does not exist", record)
        elif opcode in OPCODES:
            for item in keys:
                source = next(iter(item.values())) if isinstance(item, dict) else item
                if source not in record:
                    utils.reject_record(f"{source} key does not exist", record)
                    break
        else:
            utils.reject_record(f"{opcode} - unsupported opcode", record)

        if not utils.is_rejected(record):
            if opcode == OpCode.CREATE.value:
                records_to_insert.append(record)
            elif opcode == OpCode.UPDATE.value:
                records_to_update.append(record)
            elif opcode == OpCode.DELETE.value:
                records_to_delete.append(record)

    return records_to_insert, records_to_update, records_to_delete


def get_column_mapping(mapping: List[Union[Dict[str, str], str]]) -> List[Dict[str, str]]:
    return [{"column": next(iter(item.keys())), "key": next(iter(item.values()))} if isinstance(item, dict)
            else {"column": item, "key": item} for item in mapping] if mapping else []


def map_record(record: Dict[str, Any],
               keys: List[Union[Dict[str, str], str]],
               mapping: List[Union[Dict[str, str], str]] = []):
    # map the record based on the mapping definitions
    # add nulls for missing mapping fields
    mapped_record = {}
    for item in keys + mapping:
        source = next(iter(item.values())) if isinstance(item, dict) else item
        target = next(iter(item.keys())) if isinstance(item, dict) else item
        mapped_record[target] = None if source not in record else record[source]

    return mapped_record
