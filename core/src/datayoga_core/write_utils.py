import logging
from itertools import groupby
from typing import Any, Dict, List, Union

from datayoga_core import utils
from datayoga_core.opcode import OpCode

logger = logging.getLogger("dy")


def group_records_by_opcode(data: List[Dict[str, Any]],
                            opcode_field: str,
                            keys: List[Union[Dict[str, str], str]]) -> List[Dict[str, Any]]:
    # group records by their opcode, reject records with unsupported opcode or with missing keys
    records_by_opcode_groups = [{"opcode": key, "records": list(result)} for key, result in groupby(
        data, key=lambda record: record.get(opcode_field, "").replace(OpCode.CREATE.value, OpCode.UPDATE.value))]

    for records_by_opcode in records_by_opcode_groups:
        opcode = records_by_opcode["opcode"]
        records: List[Dict[str, Any]] = records_by_opcode["records"]

        logger.debug(f"Total {len(records)} record(s) with {opcode} opcode")
        for record in records:
            if opcode in [OpCode.UPDATE.value, OpCode.DELETE.value]:
                for item in keys:
                    source = next(iter(item.values())) if isinstance(item, dict) else item
                    if source not in record:
                        utils.reject_record(f"{source} key does not exist", record)
                        break
            else:
                utils.reject_record(f"{opcode} - unsupported opcode", record)

    return records_by_opcode_groups


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
