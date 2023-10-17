import pytest
from datayoga_core import utils
from datayoga_core.blocks.add_field.block import Block


@pytest.mark.asyncio
async def test_add_field():
    """Test case for adding a field using JMESPath expression."""
    block = Block({
        "field": "full_name",
        "language": "jmespath",
        "expression": "[fname, lname] | join(' ', @)"
    })
    block.init()

    assert await block.run([
        {"fname": "john", "lname": "doe"}
    ]) == utils.all_success([
        {"fname": "john", "lname": "doe", "full_name": "john doe"}
    ])


@pytest.mark.asyncio
async def test_add_nested_field():
    """Test case for adding a nested field using JMESPath expression."""
    block = Block({
        "field": "name.full_name",
        "language": "jmespath",
        "expression": "[name.fname, name.lname] | join(' ', @)"
    })
    block.init()

    assert await block.run([
        {"name": {"fname": "john", "lname": "doe"}}
    ]) == utils.all_success([
        {"name": {"fname": "john", "lname": "doe", "full_name": "john doe"}}
    ])


@pytest.mark.asyncio
async def test_add_nested_field_parent_key_missing():
    """Test case for adding a nested field when the parent key is missing."""
    block = Block({
        "field": "location.street",
        "language": "jmespath",
        "expression": "lname"
    })
    block.init()

    assert await block.run([
        {
            "fname": "jane",
            "lname": "smith",
            "country_code": 1,
            "country_name": "usa",
            "credit_card": "1234-5678-0000-9999",
            "gender": "F"
        }
    ]) == utils.all_success([
        {
            "fname": "jane",
            "lname": "smith",
            "country_code": 1,
            "country_name": "usa",
            "credit_card": "1234-5678-0000-9999",
            "location": {"street": "smith"},
            "gender": "F"
        }
    ])


@pytest.mark.asyncio
async def test_add_multiple_fields():
    """Test case for adding multiple fields using JMESPath expressions."""
    block = Block({
        "fields": [
            {
                "field": "name.full_name",
                "language": "jmespath",
                "expression": "concat([name.fname, ' ', name.lname])"
            },
            {
                "field": "name.fname_upper",
                "language": "jmespath",
                "expression": "upper(name.fname)"
            }
        ]
    })
    block.init()

    assert await block.run([
        {
            "name": {
                "fname": "john",
                "lname": "doe"
            }
        }
    ]) == utils.all_success([
        {
            "name": {
                "fname": "john",
                "lname": "doe",
                "full_name": "john doe",
                "fname_upper": "JOHN"
            }
        }
    ])


@pytest.mark.asyncio
async def test_add_field_with_dot():
    """Test case for adding a field with a dot in the name using JMESPath expression."""
    block = Block({
        "field": "name\.full_name",
        "language": "jmespath",
        "expression": "[fname, lname] | join(' ', @)"
    })
    block.init()

    assert await block.run([
        {"fname": "john", "lname": "doe"}
    ]) == utils.all_success([
        {"fname": "john", "lname": "doe", "name.full_name": "john doe"}
    ])
