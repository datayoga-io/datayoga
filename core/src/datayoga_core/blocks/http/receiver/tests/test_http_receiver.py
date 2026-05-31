import asyncio

import aiohttp
import pytest
from datayoga_core.blocks.http.receiver.block import Block


def _free_port():
    """Returns an unused TCP port on localhost."""
    import socket
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.mark.asyncio
async def test_http_receiver_batches_incoming_requests():
    """60 POSTs with batch_size=50 + flush_ms=200 yield at least one full batch of 50."""
    port = _free_port()
    block = Block({"host": "127.0.0.1", "port": port,
                   "batch_size": 50, "flush_ms": 200})
    block.init()

    received = []
    gen = block.produce()

    async def consumer():
        """Drains the producer until 60 records have arrived, then closes the generator."""
        try:
            async for batch in gen:
                received.append(batch)
                if sum(len(b) for b in received) >= 60:
                    return
        finally:
            await gen.aclose()

    consumer_task = asyncio.create_task(consumer())
    await asyncio.sleep(0.2)  # let server start

    async with aiohttp.ClientSession() as session:
        for i in range(60):
            async with session.post(f"http://127.0.0.1:{port}", json={"i": i}) as r:
                assert r.status == 200

    await asyncio.wait_for(consumer_task, timeout=5)

    flat = [r for b in received for r in b]
    assert len(flat) == 60
    assert any(len(b) == 50 for b in received)
    assert all(Block.MSG_ID_FIELD in r for r in flat)
