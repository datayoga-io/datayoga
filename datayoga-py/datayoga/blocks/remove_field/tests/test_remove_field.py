from datayoga.blocks.remove_field.block import Block
import pytest


@pytest.mark.asyncio
async def test_remove_existing_field():
    block = Block({"field": "fname"})
    assert await block.run([{"fname": "john", "lname": "doe"}]) == [{"lname": "doe"}]


@pytest.mark.asyncio
async def test_remove_missing_field():
    block = Block({"field": "mname"})
    assert await block.run([{"fname": "john", "lname": "doe"}]) == [{"fname": "john", "lname": "doe"}]


@pytest.mark.asyncio
async def test_remove_deep_nested_field():
    block = Block({"field": "employee.name.fname"})
    assert await block.run([{"employee": {"name": {"fname": "john", "lname": "doe"}}}]) == [
        {"employee": {"name": {"lname": "doe"}}}]


@pytest.mark.asyncio
async def test_remove_nested_field():
    block = Block({"field": "name.fname"})
    assert await block.run([{"name": {"fname": "john", "lname": "doe"}}]) == [{"name": {"lname": "doe"}}]


@pytest.mark.asyncio
async def test_remove_missing_nested_field():
    block = Block({"field": "full_name.fname"})
    assert await block.run([{"name": {"fname": "john", "lname": "doe"}}]) == [{"name": {"fname": "john", "lname": "doe"}}]


@pytest.mark.asyncio
async def test_remove_multiple_fields():
    block = Block({"fields": [{"field": "name.fname"}, {"field": "name.lname"}]})
    assert await block.run([{"name": {"fname": "john", "lname": "doe"}}]) == [{"name": {}}]


@pytest.mark.asyncio
async def test_remove_field_with_dot():
    block = Block({"field": "name\.fname"})
    assert await block.run([{"name.fname": "john", "lname": "doe"}]) == [{"lname": "doe"}]
