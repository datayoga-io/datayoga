import pytest
from datayoga_core import utils
from datayoga_core.blocks.add_field.block import Block
from datayoga_core.result import Status


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


@pytest.mark.asyncio
async def test_single_record_failure():
    """Test case showing that JSON parse failures on some records shouldn't fail the entire batch."""
    block = Block({
        "field": "parsed",
        "language": "jmespath",
        "expression": "json_parse(JSON_FORMAT)"
    })
    block.init()

    test_data = [
        {"JSON_FORMAT": '{"valid": "json1"}'},
        {"JSON_FORMAT": "{invalid_json1"},
        {"JSON_FORMAT": '{"valid": "json2"}'},
        {"JSON_FORMAT": "{invalid_json2"},
        {"JSON_FORMAT": '{"valid": "json3"}'},
        {"JSON_FORMAT": '{"name": "test"}'}
    ]

    result = await block.run(test_data)

    # Check counts
    assert len(result.processed) == 4  # Should have 4 successful records
    assert len(result.rejected) == 2   # Should have 2 rejected records
    assert len(result.filtered) == 0

    # Check processed records
    for i, record in enumerate(result.processed):
        assert record.status == Status.SUCCESS
        if i == 0:
            assert record.payload["parsed"] == {"valid": "json1"}
        elif i == 1:
            assert record.payload["parsed"] == {"valid": "json2"}
        elif i == 2:
            assert record.payload["parsed"] == {"valid": "json3"}
        elif i == 3:
            assert record.payload["parsed"] == {"name": "test"}

    # Check rejected records
    for record in result.rejected:
        assert record.status == Status.REJECTED
        assert "invalid_json" in record.payload["JSON_FORMAT"]
        assert record.message  # Should contain JSON parse error
