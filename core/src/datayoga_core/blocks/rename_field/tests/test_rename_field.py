import pytest
from datayoga_core import utils
from datayoga_core.blocks.rename_field.block import Block


@pytest.mark.asyncio
async def test_rename_existing_field():
    block = Block(
        {
            "from_field": "fname",
            "to_field": "first_name"
        }
    )
    block.init()
    assert await block.run([{"fname": "john", "lname": "doe"}]) == utils.all_success([{"first_name": "john", "lname": "doe"}])


@pytest.mark.asyncio
async def test_rename_missing_field():
    block = Block(
        {
            "from_field": "mname",
            "to_field": "middle_name"
        }
    )
    block.init()
    assert await block.run([{"fname": "john", "lname": "doe"}]) == utils.all_success([
        {"fname": "john", "lname": "doe"}])


@pytest.mark.asyncio
async def test_rename_deep_nested_field():
    block = Block(
        {
            "from_field": "employee.name.fname",
            "to_field": "employee.name.first_name"
        }
    )
    block.init()
    assert await block.run([{"employee": {"name": {"fname": "john", "lname": "doe"}}}]) == utils.all_success([

        {"employee": {"name": {"first_name": "john", "lname": "doe"}}}])


@pytest.mark.asyncio
async def test_rename_nested_field():
    block = Block(
        {
            "from_field": "name.fname",
            "to_field": "name.first_name"
        }
    )
    block.init()
    assert await block.run([{"name": {"fname": "john", "lname": "doe"}}]) == utils.all_success([
        {"name": {"first_name": "john", "lname": "doe"}}])


@pytest.mark.asyncio
async def test_rename_nested_field_missing_to_field_parent():
    block = Block(
        {
            "from_field": "name.fname",
            "to_field": "new_name.first_name"
        }
    )
    block.init()
    assert await block.run([{"name": {"fname": "john", "lname": "doe"}}]) == utils.all_success([
        {"name": {"lname": "doe"}, "new_name": {"first_name": "john"}}])


@pytest.mark.asyncio
async def test_rename_field_with_dot():
    block = Block(
        {
            "from_field": "name\.fname",
            "to_field": "name\.first_name"
        }
    )
    block.init()
    assert await block.run([{"name.fname": "john", "lname": "doe"}]) == utils.all_success([{"name.first_name": "john", "lname": "doe"}])
