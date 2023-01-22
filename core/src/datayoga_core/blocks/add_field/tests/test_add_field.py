import pytest
from datayoga_core import result, utils
from datayoga_core.blocks.add_field.block import Block


@pytest.mark.asyncio
async def test_add_field():
    block = Block({"field": "full_name",
                   "language": "jmespath",
                   "expression": "[fname,lname] | join(' ', @)"})
    block.init()
    assert await block.run([{"fname": "john", "lname": "doe"}]) == utils.block_result_success([
        {"fname": "john", "lname": "doe", "full_name": "john doe"}]
    )


@pytest.mark.asyncio
async def test_add_nested_field():
    block = Block({"field": "name.full_name",
                   "language": "jmespath",
                  "expression": "[name.fname,name.lname] | join(' ', @)"})
    block.init()
    assert await block.run([{"name": {"fname": "john", "lname": "doe"}}]) == utils.block_result_success([
        {"name": {"fname": "john", "lname": "doe", "full_name": "john doe"}}])


@pytest.mark.asyncio
async def test_add_multiple_fields():
    block = Block({"fields": [{"field": "name.full_name", "language": "jmespath", "expression": "concat([name.fname, ' ', name.lname])"}, {
                  "field": "name.fname_upper", "language": "jmespath", "expression": "upper(name.fname)"}]})
    block.init()
    assert await block.run([{"name": {"fname": "john", "lname": "doe"}}]) == utils.block_result_success([
        {"name": {"fname": "john", "lname": "doe", "full_name": "john doe", "fname_upper": "JOHN"}}])


@pytest.mark.asyncio
async def test_add_field_with_dot():
    block = Block({"field": "name\.full_name",
                   "language": "jmespath",
                   "expression": "[fname,lname] | join(' ', @)"})
    block.init()
    assert await block.run([{"fname": "john", "lname": "doe"}]) == utils.block_result_success([
        {"fname": "john", "lname": "doe", "name.full_name": "john doe"}])
