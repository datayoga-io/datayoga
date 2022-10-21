import aiofastforward
from unittest import mock
import datetime
import pytest
import time
from step import Step
import mock
import asyncio


class SleepBlock():
    async def run(self, i):
        await asyncio.sleep(i[0]["sleep"])
        return i


async def main():
    # tests that step A does not wait for B to complete before processing the next item, up to a limit
    start = datetime.datetime.now()
    results_block = mock.AsyncMock()
    loop = asyncio.get_event_loop()
    root = Step("A", SleepBlock(), concurrency=2)
    root | Step("C", results_block, concurrency=100)
    input = [
        {'key': 0, 'value': {'key': 0, 'sleep': 2}},
        {'key': 1, 'value': {'key': 1, 'sleep': 1}},
        {'key': 2, 'value': {'key': 2, 'sleep': 2}},
        {'key': 3, 'value': {'key': 3, 'sleep': 0.5}}
    ]

    # we expect these to return in pairs where the shorter one in the pair returns first
    start = loop.time()
    for i in input:
        await root.process([i])
    print("forwarded")
    await root.join()
    await root.stop()
    print(loop.time()-start)

asyncio.run(main())
