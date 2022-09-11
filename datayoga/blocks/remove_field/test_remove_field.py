from datayoga.blocks.remove_field.block import Block


def test_remove_existing_field():
    block = Block(
        {
            "field": "fname",
        }
    )
    assert block.run([{"fname": "john", "lname": "doe"}]) == [{"lname": "doe"}]


def test_remove_missing_field():
    block = Block(
        {
            "field": "mname",
        }
    )
    assert block.run([{"fname": "john", "lname": "doe"}]) == [{"fname": "john", "lname": "doe"}]


def test_remove_deep_nested_field():
    block = Block(
        {
            "field": "employee.name.fname",
        }
    )
    assert block.run([{"employee": {"name": {"fname": "john", "lname": "doe"}}}]) == [
        {"employee": {"name": {"lname": "doe"}}}]


def test_remove_nested_field():
    block = Block(
        {
            "field": "name.fname"
        }
    )
    assert block.run([{"name": {"fname": "john", "lname": "doe"}}]) == [{"name": {"lname": "doe"}}]


def test_remove_missing_nested_field():
    block = Block(
        {
            "field": "full_name.fname",
        }
    )
    assert block.run([{"name": {"fname": "john", "lname": "doe"}}]) == [{"name": {"fname": "john", "lname": "doe"}}]


def test_remove_multiple_fields():
    block = Block({"fields": [{"field": "name.fname"}, {"field": "name.lname"}]})
    assert block.run([{"name": {"fname": "john", "lname": "doe"}}]) == [{"name": {}}]
