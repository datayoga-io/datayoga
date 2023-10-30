import pytest
from datayoga_core import utils
from datayoga_core.blocks.jinja_template.block import Block


@pytest.mark.asyncio
async def test_jinja_template():
    """Test case for applying Jinja template to a field."""
    block = Block({
        "field": "full_name",
        "template": "{{ fname }} {{ lname }}"
    })
    block.init()

    assert await block.run([
        {"fname": "john", "lname": "doe"}
    ]) == utils.all_success([
        {"fname": "john", "lname": "doe", "full_name": "john doe"}
    ])


@pytest.mark.asyncio
async def test_jinja_template_nested_field():
    """Test case for applying Jinja template to a nested field."""
    block = Block({
        "field": "name.full_name",
        "template": "{{ name.fname }} {{ name.lname }}"
    })
    block.init()

    assert await block.run([
        {"name": {"fname": "john", "lname": "doe"}}
    ]) == utils.all_success([
        {"name": {"fname": "john", "lname": "doe", "full_name": "john doe"}}
    ])


@pytest.mark.asyncio
async def test_jinja_template_with_dot():
    """Test case for adding a field with a dot in the name using Jinja template."""
    block = Block({
        "field": "name\.full_name",
        "template": "{{ fname }} {{ lname }}"
    })
    block.init()

    assert await block.run([
        {"fname": "john", "lname": "doe"}
    ]) == utils.all_success([
        {"fname": "john", "lname": "doe", "name.full_name": "john doe"}
    ])


@pytest.mark.asyncio
async def test_jinja_template_missing_field():
    """Test cases for handling missing fields in Jinja template."""
    block = Block({
        "field": "greeting",
        "template": "Hello {{ fname | upper }} {{ lname }}!"
    })
    block.init()

    assert await block.run([
        {"lname": "doe"},
        {"fname": "john"},
        {"fname": "john", "lname": "doe"},
        {}
    ]) == utils.all_success([
        {"lname": "doe", "greeting": "Hello  doe!"},
        {"fname": "john", "greeting": "Hello JOHN !"},
        {"fname": "john", "lname": "doe", "greeting": "Hello JOHN doe!"},
        {"greeting": "Hello  !"}
    ])
