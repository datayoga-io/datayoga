from unittest.mock import patch

import orjson
import pytest

from datayoga_core.blocks.std.read.block import Block


async def _drain(producer):
    """Collects all batches emitted by a producer until end-of-stream."""
    out = []
    async for batch in producer.produce():
        out.append(batch)
    return out


@pytest.mark.asyncio
async def test_std_read_batches_to_batch_size():
    payload = [{"i": i} for i in range(2500)]
    fake_stdin = [orjson.dumps(payload).decode()]

    block = Block({"batch_size": 1000})
    block.init()

    with patch("datayoga_core.blocks.std.read.block.select.select",
               return_value=([object()], [], [])), \
         patch("datayoga_core.blocks.std.read.block.sys.stdin", fake_stdin):
        batches = await _drain(block)

    assert [len(b) for b in batches] == [1000, 1000, 500]
    flat = [r for b in batches for r in b]
    assert flat[0]["i"] == 0
    assert all(Block.MSG_ID_FIELD in r for r in flat)
