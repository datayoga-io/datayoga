import asyncio
import datetime
import pytest
import time
from step import Step
import mock
import aiofastforward


class SleepBlock():
    async def run(self, i):
        await asyncio.sleep(i[0]["sleep"])
        return i


@pytest.mark.asyncio
async def test_step_continuous_in_order():
    start = datetime.datetime.now()
    results_block = mock.AsyncMock()
    root = Step("A", SleepBlock(), concurrency=1)
    root | Step("B", results_block, concurrency=1)
    input = [{"key": k, "value": {"key": k, "sleep": v}} for k, v in enumerate([0.3, 0.4, 0.5, 1])]
    for i in input:
        await root.process([i])
    await root.stop()
    results_block.assert_has_calls([mock.call.run([i["value"]]) for i in input])


@pytest.mark.asyncio
async def test_step_continuous_parallel():
    # tests that step A does not wait for B to complete before processing the next item, up to a limit
    loop = asyncio.get_event_loop()
    start = loop.time()
    results_block = mock.AsyncMock()
    root = Step("A", SleepBlock(), concurrency=1)
    root | Step("B", SleepBlock(), concurrency=1) | Step("C", results_block, concurrency=1)
    input = [{"key": k, "value": {"key": k, "sleep": v}} for k, v in enumerate([0.5, 0.5, 0.5, 0.5])]

    for i in input:
        await root.process([i])
    await root.stop()
    results_block.assert_has_calls([mock.call.run([i["value"]]) for i in input])
    # if we are parallel with 1 worker, we should 4*interval vs 4*2*interval if we were sequential
    assert round(loop.time()-start, 1) == 0.5*5


@pytest.mark.asyncio
async def test_step_parallel():
    # test with parallel async workers
    loop = asyncio.get_event_loop()
    start = loop.time()
    results_block = mock.AsyncMock()
    root = Step("A", SleepBlock(), concurrency=2)
    root | Step("C", results_block, concurrency=2)
    input = [
        {'key': 0, 'value': {'key': 0, 'sleep': 0.6}},
        {'key': 1, 'value': {'key': 1, 'sleep': 0.4}},
        {'key': 2, 'value': {'key': 2, 'sleep': 0.6}},
        {'key': 3, 'value': {'key': 3, 'sleep': 0.3}}
    ]

    for i in input:
        await root.process([i])
    await root.stop()
    # we expect these to return in pairs where the shorter one in the pair returns first
    expected_output = [
        {'key': 1, 'value': {'key': 1, 'sleep': 0.4}},
        {'key': 0, 'value': {'key': 0, 'sleep': 0.6}},
        {'key': 3, 'value': {'key': 3, 'sleep': 0.3}},
        {'key': 2, 'value': {'key': 2, 'sleep': 0.6}}
    ]
    results_block.assert_has_calls([mock.call.run([i["value"]]) for i in expected_output])
    # if we are parallel with 2 workers, total time should be the two slower activities
    assert round(loop.time()-start, 1) == 0.6+0.4

# test failure of a block propagates upward
