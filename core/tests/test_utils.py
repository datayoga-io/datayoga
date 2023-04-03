from datayoga_core import utils

RECORDS = [
    {"id": 1, "opcode": "r", "lines": [{"a": 1}, {"a": 2}]},
    {"id": 2, "opcode": "r", "lines": [{"a": 3}, {"a": 4}]}
]


def test_explode_records():
    exploded_records = utils.explode_records(RECORDS, "order_line: lines[]")
    assert exploded_records == [
        {"id": 1, "opcode": "r", "lines": [{"a": 1}, {"a": 2}], "order_line": {"a": 1}},
        {"id": 1, "opcode": "r", "lines": [{"a": 1}, {"a": 2}], "order_line": {"a": 2}},
        {"id": 2, "opcode": "r", "lines": [{"a": 3}, {"a": 4}], "order_line": {"a": 3}},
        {"id": 2, "opcode": "r", "lines": [{"a": 3}, {"a": 4}], "order_line": {"a": 4}}
    ]


def test_explode_records_specific_nested_field():
    exploded_records = utils.explode_records(RECORDS, "order_line: lines[].a")
    assert exploded_records == [
        {"id": 1, "opcode": "r", "lines": [{"a": 1}, {"a": 2}], "order_line":  1},
        {"id": 1, "opcode": "r", "lines": [{"a": 1}, {"a": 2}], "order_line":  2},
        {"id": 2, "opcode": "r", "lines": [{"a": 3}, {"a": 4}], "order_line":  3},
        {"id": 2, "opcode": "r", "lines": [{"a": 3}, {"a": 4}], "order_line":  4}
    ]


def test_explode_records_override_field():
    exploded_records = utils.explode_records(RECORDS, "lines: lines[]")
    assert exploded_records == [
        {"id": 1, "opcode": "r", "lines": {"a": 1}},
        {"id": 1, "opcode": "r", "lines": {"a": 2}},
        {"id": 2, "opcode": "r", "lines": {"a": 3}},
        {"id": 2, "opcode": "r", "lines": {"a": 4}}
    ]


def test_explode_records_missing_field():
    records = [
        {"id": 1, "opcode": "r", "lines": [{"a": 1}, {"a": 2}]},
        {"id": 2, "opcode": "r", "x": [{"a": 3}, {"a": 4}]}
    ]
    exploded_records = utils.explode_records(records, "lines: lines[]")
    assert exploded_records == [
        {"id": 1, "opcode": "r", "lines": {"a": 1}},
        {"id": 1, "opcode": "r", "lines": {"a": 2}}]
