from datayoga.blocks.filter.block import Block


def test_filter_sql():
    block = Block({"language": "sql",
                   "expression": "age>20"})
    assert block.run([{"fname": "john", "lname": "doe", "age": 25}]) == [{"fname": "john", "lname": "doe", "age": 25}]


def test_filter_sql_not():
    block = Block({"language": "sql",
                   "expression": "age>20"})
    assert block.run([{"fname": "john", "lname": "doe", "age": 15}]) == []


def test_filter_jmespath():
    block = Block({"language": "jmespath",
                   "expression": "age>`20`"})
    assert block.run([{"fname": "john", "lname": "doe", "age": 25}]) == [{"fname": "john", "lname": "doe", "age": 25}]


def test_filter_jmespath_not():
    block = Block({"language": "jmespath",
                   "expression": "age>`20`"})
    assert block.run([{"fname": "john", "lname": "doe", "age": 15}]) == []
