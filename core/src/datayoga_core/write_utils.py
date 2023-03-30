import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple, Union

from datayoga_core import utils
from datayoga_core.result import Result, Status

logger = logging.getLogger("dy")


def validate_records(records: List[Dict[str, Any]], keys: List[str]) -> Tuple[List[Dict[str, Any]], List[Result]]:

    # validate that the specified keys exist in the records
    rejected_records: List[Result] = []
    valid_records = []
    key_set = set(keys)
    for record in records:
        if key_set - record.keys():
            # not all keys are present. add to rejected
            rejected_records.append(
                Result(status=Status.REJECTED, payload=record, message=f"missing {key_set-record.keys()} field(s)")
            )
        else:
            # all keys are present
            valid_records.append(record)
    return valid_records, rejected_records


def group_records_by_opcode(records: List[Dict[str, Any]],
                            opcode_field: str) -> Dict[str, List[Dict[str, Any]]]:
    # group records by their opcode, reject records with unsupported opcode or with missing keys
    groups = defaultdict(list)
    for record in records:
        groups[record.get(opcode_field, "")].append(record)
    return groups


def get_column_mapping(mapping: List[Union[Dict[str, str], str]]) -> List[Dict[str, str]]:
    return [{"column": next(iter(item.keys())), "key": next(iter(item.values()))} if isinstance(item, dict)
            else {"column": item, "key": item} for item in mapping] if mapping else []


def map_record(record: Dict[str, Any],
               keys: List[Union[Dict[str, str], str]],
               mapping: Optional[List[Union[Dict[str, str], str]]] = None):
    # map the record based on the mapping definitions
    # add nulls for missing mapping fields
    mapping = mapping or []
    mapped_record = {}
    for item in keys + mapping:
        source = next(iter(item.values())) if isinstance(item, dict) else item

        # columns with spaces will be later used with underscores in the bind variables
        target = (next(iter(item.keys())) if isinstance(item, dict) else item).replace(" ", "_")

        source_path = utils.split_field(source)
        obj = record
        for key in source_path:
            key = utils.unescape_field(key)
            if key in obj:
                obj = obj[key]

        mapped_record[target] = obj if obj != record else None

    return mapped_record
