import pytest
from datayoga_core import result, utils
from datayoga_core.blocks.filter.block import Block


@pytest.mark.asyncio
async def test_filter_sql():
    block = Block(
        {
            "language": "sql",
            "expression": "age>20"
        }
    )
    block.init()
    assert await block.run([{"fname": "john", "lname": "doe", "age": 25}]) == utils.all_success([{"fname": "john", "lname": "doe", "age": 25}])


@pytest.mark.asyncio
async def test_filter_sql_multiple():
    block = Block(
        {
            "language": "sql",
            "expression": "age>20"
        }
    )
    block.init()
    assert tuple(await block.run(
        [
            {"fname": "john", "lname": "doe", "age": 25},
            {"fname": "john2", "lname": "doe2", "age": 15},
            {"fname": "john3", "lname": "doe3", "age": 22}
        ]
    )) == (
        [
            result.Result(status=result.Status.SUCCESS, payload={"fname": "john", "lname": "doe", "age": 25}),
            result.Result(status=result.Status.SUCCESS, payload={"fname": "john3", "lname": "doe3", "age": 22})
        ],
        [
            result.Result(status=result.Status.FILTERED, payload={"fname": "john2", "lname": "doe2", "age": 15})
        ],
        []
    )


@pytest.mark.asyncio
async def test_filter_jmespath_multiple():
    block = Block(
        {
            "language": "jmespath",
            "expression": "age>`20`"
        }
    )
    block.init()
    assert tuple(await block.run(
        [
            {"fname": "john", "lname": "doe", "age": 25},
            {"fname": "john2", "lname": "doe2", "age": 15},
            {"fname": "john3", "lname": "doe3", "age": 22}
        ]
    )) == (
        [
            result.Result(status=result.Status.SUCCESS, payload={"fname": "john", "lname": "doe", "age": 25}),
            result.Result(status=result.Status.SUCCESS, payload={"fname": "john3", "lname": "doe3", "age": 22})
        ],
        [
            result.Result(status=result.Status.FILTERED, payload={"fname": "john2", "lname": "doe2", "age": 15})
        ],
        []
    )


@pytest.mark.asyncio
async def test_filter_sql_not():
    block = Block(
        {
            "language": "sql",
            "expression": "age>20"
        }
    )
    block.init()
    assert tuple(await block.run([{"fname": "john", "lname": "doe", "age": 15}])) == ([], [result.Result(status=result.Status.FILTERED, payload={"fname": "john", "lname": "doe", "age": 15})], [])


@pytest.mark.asyncio
async def test_filter_jmespath():
    block = Block(
        {
            "language": "jmespath",
            "expression": "age>`20`"
        }
    )
    block.init()
    assert await block.run([{"fname": "john", "lname": "doe", "age": 25}]) == utils.all_success([{"fname": "john", "lname": "doe", "age": 25}])


@pytest.mark.asyncio
async def test_filter_jmespath_multiple_nested():
    block = Block(
        {
            "language": "jmespath",
            "expression": "age.years>`20`"
        }
    )
    block.init()
    assert tuple(await block.run(
        [
            {"fname": "john", "lname": "doe", "age": {"years": 25, "months": 5}},
            {"fname": "john2", "lname": "doe2", "age": {"years": 15, "months": 1}},
            {"fname": "john3", "lname": "doe3", "age": {"years": 22, "months": 2}}
        ]
    )) == (
        [
            result.Result(status=result.Status.SUCCESS, payload={"fname": "john", "lname": "doe", "age": {"years": 25, "months": 5}}),
            result.Result(status=result.Status.SUCCESS, payload={"fname": "john3", "lname": "doe3", "age": {"years": 22, "months": 2}})
        ],
        [
            result.Result(status=result.Status.FILTERED, payload={"fname": "john2", "lname": "doe2", "age": {"years": 15, "months": 1}})
        ],
        []
    )


@pytest.mark.asyncio
async def test_filter_jmespath_not():
    block = Block(
        {
            "language": "jmespath",
            "expression": "age>`20`"
        }
    )
    block.init()
    assert tuple(await block.run([{"fname": "john", "lname": "doe", "age": 15}])) == ([], [result.Result(status=result.Status.FILTERED, payload={"fname": "john", "lname": "doe", "age": 15})], [])
