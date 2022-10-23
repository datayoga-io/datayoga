import asyncio
import datetime
from wsgiref import validate
import pytest
import time
from step import Step, StepResult
import mock


class SleepBlock():
    async def run(self, i):
        await asyncio.sleep(i[0]["sleep"])
        return i


class ExceptionBlock():
    async def run(self, i):
        if (i[0]):
            raise ValueError()
        else:
            return i


@pytest.mark.asyncio
async def test_step_continuous_in_order():
    start = datetime.datetime.now()
    results_block = mock.AsyncMock()
    root = Step("A", SleepBlock(), concurrency=1)
    root | Step("B", results_block, concurrency=1)
    input = [{"msg_id": k, "value": {"key": k, "sleep": v}} for k, v in enumerate([0.3, 0.4, 0.5, 1])]
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
    input = [{"msg_id": k, "value": {"key": k, "sleep": v}} for k, v in enumerate([0.5, 0.5, 0.5, 0.5])]

    for i in input:
        await root.process([i])
    await root.stop()
    results_block.assert_has_calls([mock.call.run([i["value"]]) for i in input])
    # if we are parallel with 1 worker, we should 4*interval vs 4*2*interval if we were sequential
    assert abs(loop.time()-start-0.5*5) < 0.3


@pytest.mark.asyncio
async def test_step_parallel():
    # test with parallel async workers
    loop = asyncio.get_event_loop()
    start = loop.time()
    results_block = mock.AsyncMock()
    root = Step("A", SleepBlock(), concurrency=2)
    root | Step("C", results_block, concurrency=2)
    input = [
        {'msg_id': 0, 'value': {'key': 0, 'sleep': 0.6}},
        {'msg_id': 1, 'value': {'key': 1, 'sleep': 0.4}},
        {'msg_id': 2, 'value': {'key': 2, 'sleep': 0.6}},
        {'msg_id': 3, 'value': {'key': 3, 'sleep': 0.3}}
    ]

    for i in input:
        await root.process([i])
    await root.stop()
    # we expect these to return in pairs where the shorter one in the pair returns first
    expected_output = [
        {'msg_id': 1, 'value': {'key': 1, 'sleep': 0.4}},
        {'msg_id': 0, 'value': {'key': 0, 'sleep': 0.6}},
        {'msg_id': 3, 'value': {'key': 3, 'sleep': 0.3}},
        {'msg_id': 2, 'value': {'key': 2, 'sleep': 0.6}}
    ]
    results_block.assert_has_calls([mock.call.run([i["value"]]) for i in expected_output])
    # if we are parallel with 2 workers, total time should be the two slower activities
    assert round(loop.time()-start, 1) == 0.6+0.4


@pytest.mark.asyncio
async def test_acks_successful():
    # test success of a block propagates upward
    root = Step("A", SleepBlock(), concurrency=1)
    input = [{"msg_id": k, "value": {"key": k, "sleep": v}} for k, v in enumerate([0.3, 0.4, 0.5, 1])]
    producer = mock.MagicMock()
    root.add_done_callback(producer.ack)
    for i in input:
        await root.process([i])
    await root.stop()
    producer.assert_has_calls([mock.call.ack([i["msg_id"]], StepResult.SUCCESS, None) for i in input])


@pytest.mark.asyncio
async def test_acks_exception():
    # test failure of a block propagates upward
    root = Step("A", ExceptionBlock(), concurrency=1)
    input = [
        {"msg_id": "message1", "value": True},
        {"msg_id": "message2", "value": True}
    ]
    producer = mock.MagicMock()
    root.add_done_callback(producer.ack)
    for i in input:
        await root.process([i])
    await root.stop()
    producer.assert_has_calls([mock.call.ack([i["msg_id"]], StepResult.REJECTED,
                              "Error in step A: ValueError()") for i in input])
