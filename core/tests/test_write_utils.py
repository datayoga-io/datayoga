import pytest
from datayoga_core import write_utils
from datayoga_core.result import Status


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


@pytest.mark.parametrize("record, source, expected", [
    # Existing simple nested key
    (
        {"value": {"id": 123}},
        "value.id",
        {"target": 123}
    ),
    # Existing deeply nested key
    (
        {"user": {"profile": {"email": "john@example.com"}}},
        "user.profile.email",
        {"target": "john@example.com"}
    ),
    # Missing simple nested key
    (
        {"value": {"x": 2}},
        "value.id",
        {"target": None}
    ),
    # Missing deeply nested key
    (
        {"user": {"profile": {"name": "John"}}},
        "user.profile.email",
        {"target": None}
    ),
    # Partially nested key
    (
        {"user": {"profile": {"name": "John"}}},
        "user.address.street",
        {"target": None}
    ),
    # Mixed case with existing and non-existing keys
    (
        {"data": {"info": {"id": 123, "active": True}}},
        "data.info.status",
        {"target": None}
    ),
    # Empty nested structure
    (
        {"user": {}},
        "user.profile",
        {"target": None}
    ),
    # Null value in nested structure
    (
        {"user": {"profile": None}},
        "user.profile.name",
        {"target": None}
    ),
    # Boolean value
    (
        {"settings": {"active": True}},
        "settings.active",
        {"target": True}
    )
])
def test_map_record_nested_key(record, source, expected):
    """Tests map_record function's handling of nested keys, including existing and missing scenarios."""
    mapped = write_utils.map_record(
        record,
        keys=[{"target": source}]
    )

    assert mapped == expected, f"Failed for record {record} with source {source}"
