from datayoga.blocks.filter.block import Block


def test_filter_sql():
    block = Block(
        {
            "language": "sql",
            "expression": "age>20"
        }
    )
    assert block.run([{"fname": "john", "lname": "doe", "age": 25}]) == [{"fname": "john", "lname": "doe", "age": 25}]


def test_filter_sql_multiple():
    block = Block(
        {
            "language": "sql",
            "expression": "age>20"
        }
    )
    assert block.run(
        [
            {"fname": "john", "lname": "doe", "age": 25},
            {"fname": "john2", "lname": "doe2", "age": 15},
            {"fname": "john3", "lname": "doe3", "age": 22}
        ]
    ) == [
        {"fname": "john", "lname": "doe", "age": 25},
        {"fname": "john3", "lname": "doe3", "age": 22}
    ]


def test_filter_sql_not():
    block = Block(
        {
            "language": "sql",
            "expression": "age>20"
        }
    )
    assert block.run([{"fname": "john", "lname": "doe", "age": 15}]) == []


def test_filter_jmespath():
    block = Block(
        {
            "language": "jmespath",
            "expression": "age>`20`"
        }
    )
    assert block.run([{"fname": "john", "lname": "doe", "age": 25}]) == [{"fname": "john", "lname": "doe", "age": 25}]


def test_filter_jmespath_multiple_nested():
    block = Block(
        {
            "language": "jmespath",
            "expression": "age.years>`20`"
        }
    )
    assert block.run(
        [
            {"fname": "john", "lname": "doe", "age": {"years": 25, "months": 5}},
            {"fname": "john2", "lname": "doe2", "age": {"years": 15, "months": 1}},
            {"fname": "john3", "lname": "doe3", "age": {"years": 22, "months": 2}}
        ]
    ) == [
        {"fname": "john", "lname": "doe", "age": {"years": 25, "months": 5}},
        {"fname": "john3", "lname": "doe3", "age": {"years": 22, "months": 2}}
    ]


def test_filter_jmespath_not():
    block = Block(
        {
            "language": "jmespath",
            "expression": "age>`20`"
        }
    )
    assert block.run([{"fname": "john", "lname": "doe", "age": 15}]) == []