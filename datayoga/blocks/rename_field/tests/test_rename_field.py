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
