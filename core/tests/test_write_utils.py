import pytest
from datayoga_core import write_utils
from datayoga_core.result import Result, Status

# def test_group_records_by_opcode_invalid_opcode():
#     change_records = [
#         {"id": 1, "opcode": "r"},
#         {"id": 2, "opcode": "x"}
#     ]
#     inserted, updated, deleted, rejected = write_utils.group_records_by_opcode(
#         change_records,
#         "opcode",

#     )


def test_validate_records_missing_key():
    change_records = [
        {"id": 1, "opcode": "r"},
        {"id": 2}
    ]
    valid, rejected = write_utils.validate_records(change_records, keys=["opcode"])
    assert valid == change_records[0:1]
    assert len(rejected) == 1
    assert rejected[0].status == Status.REJECTED
    assert rejected[0].payload == change_records[1]


def test_validate_records():
    change_records = [
        {"id": 1, "opcode": "r"},
        {"id": 2, "opcode": "r", "extra": "field"}
    ]
    valid, rejected = write_utils.validate_records(change_records, keys=["opcode"])
    assert valid == change_records
    assert len(rejected) == 0


def test_group_by_opcode_invalid_opcode():
    change_records = [
        {"id": 1, "opcode": "r"},
        {"id": 2, "opcode": "x", "extra": "field"}
    ]
    groups = write_utils.group_records_by_opcode(change_records, opcode_field="opcode")
    assert groups["r"] == change_records[0:1]
    assert groups["x"] == change_records[1:2]
