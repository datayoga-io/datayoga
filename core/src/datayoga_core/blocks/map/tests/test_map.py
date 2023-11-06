import pytest
from datayoga_core import utils
from datayoga_core.blocks.map.block import Block


@pytest.mark.asyncio
async def test_map_expression_jmespath():
    block = Block(properties={"language": "jmespath",
                              "expression": """{
                        "new_field": `hello`
                   }"""})
    block.init()
    assert await block.run([{"fname": "john", "lname": "doe"}]) == utils.all_success([
        {"new_field": "hello"}]
    )


@pytest.mark.asyncio
async def test_map_field_multiple_expressions_jmespath():
    block = Block(properties={"language": "jmespath",
                              "expression": """{
                            "new_field": `hello`, "name" : fname
                    }"""})
    block.init()
    assert await block.run([{"fname": "john", "lname": "doe"}]) == utils.all_success([
        {"new_field": "hello", "name": "john"}]
    )


@pytest.mark.asyncio
async def test_map_field_nested_expression_jmespath():
    block = Block(properties={"language": "jmespath",
                              "expression": """{
                            "new_field": `hello`, "name" : details.fname
                    }"""})
    block.init()
    assert await block.run([{"details": {"fname": "john", "lname": "doe"}}]) == utils.all_success([
        {"new_field": "hello", "name": "john"}]
    )


@pytest.mark.asyncio
async def test_map_field_double_nested_expression_jmespath():
    block = Block(properties={"language": "jmespath",
                              "expression": """{
                            "new_field": `hello`, "name" : details.name.fname
                    }"""})
    block.init()
    assert await block.run([{"details": {"name": {"fname": "john", "lname": "doe"}, "country": "israel"}}]) == utils.all_success([
        {"new_field": "hello", "name": "john"}]
    )


@pytest.mark.asyncio
async def test_map_field_nested_expression_jmespath_object():
    block = Block(properties={"language": "jmespath", "expression": {"new_field": "`hello`", "name": "details.fname"}})
    block.init()
    assert await block.run([{"details": {"fname": "john", "lname": "doe"}}]) == utils.all_success([
        {"new_field": "hello", "name": "john"}]
    )


@pytest.mark.asyncio
async def test_map_field_double_nested_expression_jmespath_object():
    block = Block(properties={"language": "jmespath", "expression": {
                  "new_field": "`hello`", "name": "details.name.fname"}})
    block.init()
    assert await block.run([{"details": {"name": {"fname": "john", "lname": "doe"}, "country": "israel"}}]) == utils.all_success([
        {"new_field": "hello", "name": "john"}]
    )


@pytest.mark.asyncio
async def test_map_expression_non_quoted_jmespath():
    block = Block(properties={"language": "jmespath",
                              "expression": {
                                  "name": "fname"
                              }})
    block.init()
    assert await block.run([{"fname": "john", "lname": "doe"}]) == utils.all_success([{"name": "john"}])


@pytest.mark.asyncio
async def test_map_multiple_expressions_non_quoted_jmespath():
    block = Block(properties={"language": "jmespath",
                              "expression": {
                                  "name": "fname", "last name": "lname"
                              }})
    block.init()
    assert await block.run([{"fname": "john", "lname": "doe"}]) == utils.all_success([
        {"name": "john", "last name": "doe"}]
    )


@pytest.mark.asyncio
async def test_map_expression_sql():
    block = Block(properties={"language": "sql",
                              "expression": {
                                  "new_field": "fname"
                              }})
    block.init()
    assert await block.run([{"fname": "john", "lname": "doe"}]) == utils.all_success([
        {"new_field": "john"}]
    )


@pytest.mark.asyncio
async def test_map_multiple_expressions_sql():
    block = Block(properties={"language": "sql",
                              "expression": {
                                  "name": "fname", "last name": "lname"
                              }})
    block.init()
    assert await block.run([{"fname": "john", "lname": "doe"}]) == utils.all_success([
        {"name": "john", "last name": "doe"}]
    )


@pytest.mark.asyncio
async def test_map_double_nested_expression_sql():
    block = Block(properties={"language": "sql",
                              "expression": {
                                  "name": "(`details.name.fname`)"
                              }})
    block.init()
    assert await block.run([{"details": {"name": {"fname": "john", "lname": "doe"}, "country": "israel"}}]) == utils.all_success([
        {"name": "john"}]
    )


@pytest.mark.asyncio
async def test_map_nested_expression_sql():
    block = Block(properties={"language": "sql",
                              "expression": {
                                  "name": "(`details.fname`)"
                              }})
    block.init()
    assert await block.run([{"details": {"fname": "john", "lname": "doe"}}]) == utils.all_success([
        {"name": "john"}]
    )


@pytest.mark.asyncio
async def test_map_malformed():
    block = Block(properties={"language": "sql",
                              "expression": "{name: (`details.fname`) "})
    with pytest.raises(ValueError):
        block.init()


@pytest.mark.asyncio
async def test_jmespath_does_not_filter_in_map():
    block = Block(properties={"language": "jmespath",
                              "expression": "{name: a} "})
    block.init()
    assert await block.run([{"a": "one"}, {"b": "two"}]) == utils.all_success([
        {"name": "one"},
        {"name": None}
    ])


@pytest.mark.asyncio
async def test_map_malformed_sql():
    block = Block(properties={"language": "sql",
                              "expression": "{name: (`details.fname`) "})
    with pytest.raises(ValueError):
        block.init()
        await block.run([{"details": {"fname": "john", "lname": "doe"}}])
