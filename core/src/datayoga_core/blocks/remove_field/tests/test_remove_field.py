import pytest
from datayoga_core import utils
from datayoga_core.blocks.remove_field.block import Block


@pytest.mark.asyncio
async def test_remove_existing_field():
    block = Block({"field": "fname"})
    block.init()
    assert await block.run([{"fname": "john", "lname": "doe"}]) == utils.all_success([{"lname": "doe"}])


@pytest.mark.asyncio
async def test_remove_missing_field():
    block = Block({"field": "mname"})
    block.init()
    assert await block.run([{"fname": "john", "lname": "doe"}]) == utils.all_success([{"fname": "john", "lname": "doe"}])


@pytest.mark.asyncio
async def test_remove_deep_nested_field():
    block = Block({"field": "employee.name.fname"})
    block.init()
    assert await block.run([{"employee": {"name": {"fname": "john", "lname": "doe"}}}]) == utils.all_success([
        {"employee": {"name": {"lname": "doe"}}}])


@pytest.mark.asyncio
async def test_remove_nested_field():
    block = Block({"field": "name.fname"})
    block.init()
    assert await block.run([{"name": {"fname": "john", "lname": "doe"}}]) == utils.all_success([{"name": {"lname": "doe"}}])


@pytest.mark.asyncio
async def test_remove_missing_nested_field():
    block = Block({"field": "full_name.fname"})
    block.init()
    assert await block.run([{"name": {"fname": "john", "lname": "doe"}}]) == utils.all_success([{"name": {"fname": "john", "lname": "doe"}}])


@pytest.mark.asyncio
async def test_remove_multiple_fields():
    block = Block({"fields": [{"field": "name.fname"}, {"field": "name.lname"}]})
    block.init()
    assert await block.run([{"name": {"fname": "john", "lname": "doe"}}]) == utils.all_success([{"name": {}}])


@pytest.mark.asyncio
async def test_remove_field_with_dot():
    block = Block({"field": "name\.fname"})
    block.init()
    assert await block.run([{"name.fname": "john", "lname": "doe"}]) == utils.all_success([{"lname": "doe"}])
