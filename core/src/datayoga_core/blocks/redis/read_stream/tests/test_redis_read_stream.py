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


def _xreadgroup_count(call):
    """Extract the count arg from an xreadgroup call regardless of kw/positional."""
    if "count" in call.kwargs:
        return call.kwargs["count"]
    if len(call.args) >= 4:
        return call.args[3]
    return None


def _xreadgroup_id(call):
    """Extract the stream-id dict value from an xreadgroup call."""
    streams = call.kwargs.get("streams") or (call.args[2] if len(call.args) >= 3 else {})
    return next(iter(streams.values())) if streams else None


@pytest.mark.asyncio
async def test_redis_new_message_read_uses_count_equal_to_batch_size():
    """xreadgroup for new messages (id='>') uses count=batch_size (closes #377)."""
    redis = MagicMock()
    payload_a = (b"1-0", {b"data": b'{"i": 1}'})
    payload_b = (b"2-0", {b"data": b'{"i": 2}'})
    redis.xreadgroup.side_effect = [
        [(b"mystream", [payload_a, payload_b])],  # pending (drained in one call, count=None)
        [(b"mystream", [])],                       # new-read empty -> exit
    ]

    block = _mk_block({"batch_size": 250, "_snapshot": True}, redis)
    batches = []
    async for b in block.produce():
        batches.append(b)

    # First call is pending (id="0"); it uses count=None (drain).
    pending_call = redis.xreadgroup.call_args_list[0]
    assert _xreadgroup_id(pending_call) == "0"
    assert _xreadgroup_count(pending_call) is None, \
        "pending read should use count=None to drain PEL in one call"

    # Subsequent new-message calls (id=">") use count=batch_size.
    new_calls = [c for c in redis.xreadgroup.call_args_list if _xreadgroup_id(c) == ">"]
    assert new_calls, "expected at least one new-message read"
    for c in new_calls:
        assert _xreadgroup_count(c) == 250, \
            f"new-message read should use count=batch_size, got count={_xreadgroup_count(c)}"


@pytest.mark.asyncio
async def test_redis_yields_records_as_a_batch_not_one_by_one():
    """A 5-record xreadgroup response yields one batch of 5, not five batches of 1."""
    redis = MagicMock()
    pages = [(f"{i}-0".encode(), {b"data": f'{{"i": {i}}}'.encode()}) for i in range(5)]
    redis.xreadgroup.side_effect = [
        [(b"mystream", pages)],     # pending drained in one call
        [(b"mystream", [])],         # new-read empty -> exit
    ]

    block = _mk_block({"batch_size": 100, "_snapshot": True}, redis)
    batches = []
    async for b in block.produce():
        batches.append(b)

    assert [len(b) for b in batches] == [5]
    assert batches[0][0]["i"] == 0


@pytest.mark.asyncio
async def test_redis_drains_full_pel_in_one_call_even_when_larger_than_batch_size():
    """Pending reads use count=None so the entire PEL drains in a single call.
    The base class re-chunks the result to batch_size. This avoids the
    Copilot-flagged pagination bug where count=batch_size + XREADGROUP id='0'
    would re-read the same first page forever (since the producer doesn't ack
    inside produce_chunks)."""
    redis = MagicMock()
    # Simulate a PEL of 20 entries returned in one xreadgroup call.
    pel = [(f"{i}-0".encode(), {b"data": f'{{"i": {i}}}'.encode()}) for i in range(20)]
    redis.xreadgroup.side_effect = [
        [(b"mystream", pel)],         # entire PEL in one call (count=None)
        [(b"mystream", [])],          # new-read empty -> exit
    ]

    block = _mk_block({"batch_size": 5, "_snapshot": True}, redis)
    batches = []
    async for b in block.produce():
        batches.append(b)

    # All 20 pending entries are delivered; the base class re-chunks them
    # to batch_size=5 → four batches of 5.
    assert [len(b) for b in batches] == [5, 5, 5, 5]
    # Only ONE pending read was made (PEL drained in one shot).
    pending_calls = [c for c in redis.xreadgroup.call_args_list if _xreadgroup_id(c) == "0"]
    assert len(pending_calls) == 1, \
        f"expected exactly 1 pending read (count=None drains all), got {len(pending_calls)}"
