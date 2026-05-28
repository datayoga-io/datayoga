from unittest.mock import MagicMock

import pytest
from datayoga_core.blocks.redis.read_stream.block import Block


def _mk_block(properties, redis_client):
    """Builds a redis/read_stream Block bypassing its real init() (mocks the Redis client)."""
    block = Block.__new__(Block)
    block.properties = properties
    block.redis_client = redis_client
    block.stream = "mystream"
    block.snapshot = properties.get("_snapshot", True)
    block.consumer_group = "g"
    block.requesting_consumer = "c"
    return block


@pytest.mark.asyncio
async def test_redis_uses_count_equal_to_batch_size():
    redis = MagicMock()
    payload_a = (b"1-0", {b"data": b'{"i": 1}'})
    payload_b = (b"2-0", {b"data": b'{"i": 2}'})
    redis.xreadgroup.side_effect = [
        [(b"mystream", [payload_a, payload_b])],  # pending
        [(b"mystream", [])],                       # nothing new -> exit
    ]

    block = _mk_block({"batch_size": 250, "_snapshot": True}, redis)
    batches = []
    async for b in block.produce():
        batches.append(b)

    assert all(c.kwargs.get("count") == 250 or (len(c.args) >= 4 and c.args[3] == 250)
               for c in redis.xreadgroup.call_args_list), \
        "xreadgroup should be called with count=batch_size"


@pytest.mark.asyncio
async def test_redis_yields_records_as_a_batch_not_one_by_one():
    redis = MagicMock()
    pages = [(f"{i}-0".encode(), {b"data": f'{{"i": {i}}}'.encode()}) for i in range(5)]
    redis.xreadgroup.side_effect = [
        [(b"mystream", pages)],
        [(b"mystream", [])],
    ]

    block = _mk_block({"batch_size": 100, "_snapshot": True}, redis)
    batches = []
    async for b in block.produce():
        batches.append(b)

    assert [len(b) for b in batches] == [5]
    assert batches[0][0]["i"] == 0
