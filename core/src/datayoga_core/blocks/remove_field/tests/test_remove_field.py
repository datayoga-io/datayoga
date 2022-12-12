import pytest
from datayoga_core.blocks.remove_field.block import Block
from datayoga_core.result import Result

RESULT_SUCCESS = Result.success()

@pytest.mark.asyncio
async def test_remove_existing_field():
    block = Block({"field": "fname"})
    block.init()
    assert await block.run([{"fname": "john", "lname": "doe"}]) == ([{"lname": "doe"}], [RESULT_SUCCESS])


@pytest.mark.asyncio
async def test_remove_missing_field():
    block = Block({"field": "mname"})
    block.init()
    assert await block.run([{"fname": "john", "lname": "doe"}]) == ([{"fname": "john", "lname": "doe"}], [RESULT_SUCCESS])


@pytest.mark.asyncio
async def test_remove_deep_nested_field():
    block = Block({"field": "employee.name.fname"})
    block.init()
    assert await block.run([{"employee": {"name": {"fname": "john", "lname": "doe"}}}]) == ([
        {"employee": {"name": {"lname": "doe"}}}], [RESULT_SUCCESS])


@pytest.mark.asyncio
async def test_remove_nested_field():
    block = Block({"field": "name.fname"})
    block.init()
    assert await block.run([{"name": {"fname": "john", "lname": "doe"}}]) == ([{"name": {"lname": "doe"}}], [RESULT_SUCCESS])


@pytest.mark.asyncio
async def test_remove_missing_nested_field():
    block = Block({"field": "full_name.fname"})
    block.init()
    assert await block.run([{"name": {"fname": "john", "lname": "doe"}}]) == ([{"name": {"fname": "john", "lname": "doe"}}], [RESULT_SUCCESS])


@pytest.mark.asyncio
async def test_remove_multiple_fields():
    block = Block({"fields": [{"field": "name.fname"}, {"field": "name.lname"}]})
    block.init()
    assert await block.run([{"name": {"fname": "john", "lname": "doe"}}]) == ([{"name": {}}], [RESULT_SUCCESS])


@pytest.mark.asyncio
async def test_remove_field_with_dot():
    block = Block({"field": "name\.fname"})
    block.init()
    assert await block.run([{"name.fname": "john", "lname": "doe"}]) == ([{"lname": "doe"}], [RESULT_SUCCESS])
