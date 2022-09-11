from datayoga.blocks.rename_field.block import Block


def test_rename_existing_field():
    block = Block(
        {
            "from_field": "fname",
            "to_field": "first_name"
        }
    )
    assert block.run([{"fname": "john", "lname": "doe"}]) == [{"first_name": "john", "lname": "doe"}]


def test_rename_missing_field():
    block = Block(
        {
            "from_field": "mname",
            "to_field": "middle_name"
        }
    )
    assert block.run([{"fname": "john", "lname": "doe"}]) == [{"fname": "john", "lname": "doe"}]


def test_rename_deep_nested_field():
    block = Block(
        {
            "from_field": "employee.name.fname",
            "to_field": "employee.name.first_name"
        }
    )
    assert block.run([{"employee": {"name": {"fname": "john", "lname": "doe"}}}]) == [
        {"employee": {"name": {"first_name": "john", "lname": "doe"}}}]


def test_rename_nested_field():
    block = Block(
        {
            "from_field": "name.fname",
            "to_field": "name.first_name"
        }
    )
    assert block.run([{"name": {"fname": "john", "lname": "doe"}}]) == [
        {"name": {"first_name": "john", "lname": "doe"}}]


def test_rename_nested_field_missing_to_field_parent():
    block = Block(
        {
            "from_field": "name.fname",
            "to_field": "new_name.first_name"
        }
    )
    assert block.run([{"name": {"fname": "john", "lname": "doe"}}]) == [
        {"name": {"lname": "doe"}, "new_name": {"first_name": "john"}}]
