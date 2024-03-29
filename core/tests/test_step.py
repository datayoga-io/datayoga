import asyncio
import logging

import mock
import pytest
from datayoga_core import utils
from datayoga_core.block import Block
from datayoga_core.result import Result, Status
from datayoga_core.step import Step
from datayoga_core.step_buffer import StepBuffer

logger = logging.getLogger("dy")


class SleepBlock(Block):
    def init(self):
        pass

    def validate(self):
        return True

    async def run(self, i):
        await asyncio.sleep(i[0]["sleep"])
        return utils.all_success(i)


class ExceptionBlock(Block):
    def init(self):
        pass

    def validate(self):
        return True

    async def run(self, i):
        if (i[0]):
            raise ValueError()
        else:
            return utils.all_success(i)


class EchoBlock(Block):
    def init(self):
        pass

    def validate(self):
        return True

    async def run(self, i):
        return utils.all_success(i)


@pytest.mark.asyncio
async def test_step_continuous_in_order():
    results_block = mock.Mock(wraps=EchoBlock())
    root = Step("A", SleepBlock(), concurrency=1)
    root | Step("B", results_block, concurrency=1)
    producer_mock = mock.MagicMock()
    root.add_done_callback(producer_mock.ack)

    messages = [{Block.MSG_ID_FIELD: k, "key": k, "sleep": v} for k, v in enumerate([0.3, 0.4, 0.5, 1])]
    for message in messages:
        await root.process([message])
    await root.stop()
    assert results_block.run.call_args_list == [mock.call.run([i]) for i in messages]
    assert producer_mock.ack.call_args_list == [mock.call.ack(
        [i[Block.MSG_ID_FIELD]], [Result(status=Status.SUCCESS, payload=i)]) for i in messages]


@pytest.mark.asyncio
async def test_step_continuous_parallel():
    # tests that step A does not wait for B to complete before processing the next item, up to a limit
    loop = asyncio.get_event_loop()
    start = loop.time()
    results_block = mock.Mock(wraps=EchoBlock())
    producer_mock = mock.MagicMock()
    root = Step("A", SleepBlock(), concurrency=1)
    root | Step("B", SleepBlock(), concurrency=1) | Step("C", results_block, concurrency=1)
    root.add_done_callback(producer_mock.ack)
    input = [{Block.MSG_ID_FIELD: k, "key": k, "sleep": v} for k, v in enumerate([0.5, 0.5, 0.5, 0.5])]

    for i in input:
        await root.process([i])
    await root.stop()
    assert results_block.run.call_args_list == [mock.call.run([i]) for i in input]
    # if we are parallel with 1 worker, we should 4*interval vs 4*2*interval if we were sequential
    assert abs(loop.time()-start-0.5*5) < 0.3


@pytest.mark.asyncio
async def test_step_parallel():
    # test with parallel async workers
    loop = asyncio.get_event_loop()
    start = loop.time()
    results_block = mock.Mock(wraps=EchoBlock())
    root = Step("A", SleepBlock(), concurrency=2)
    root | Step("C", results_block, concurrency=2)
    messages = [
        {Block.MSG_ID_FIELD: 0, 'key': 0, 'sleep': 0.6},
        {Block.MSG_ID_FIELD: 1, 'key': 1, 'sleep': 0.4},
        {Block.MSG_ID_FIELD: 2, 'key': 2, 'sleep': 0.6},
        {Block.MSG_ID_FIELD: 3, 'key': 3, 'sleep': 0.3}
    ]

    for i in messages:
        await root.process([i])
    await root.stop()
    # we expect these to return in pairs where the shorter one in the pair returns first
    expected_output = [
        {Block.MSG_ID_FIELD: 1, 'key': 1, 'sleep': 0.4},
        {Block.MSG_ID_FIELD: 0, 'key': 0, 'sleep': 0.6},
        {Block.MSG_ID_FIELD: 3, 'key': 3, 'sleep': 0.3},
        {Block.MSG_ID_FIELD: 2, 'key': 2, 'sleep': 0.6}
    ]
    assert results_block.run.call_args_list == [mock.call.run([i]) for i in expected_output]
    # if we are parallel with 2 workers, total time should be the two slower activities
    assert round(loop.time()-start, 1) == 0.6+0.4


@pytest.mark.asyncio
async def test_acks_successful():
    # test success of a block propagates upward
    root = Step("A", SleepBlock(), concurrency=1)
    messages = [{Block.MSG_ID_FIELD: k, "key": k, "sleep": v} for k, v in enumerate([0.3, 0.4, 0.5, 1])]
    producer = mock.MagicMock()
    root.add_done_callback(producer.ack)
    for message in messages:
        await root.process([message])
    logger.debug("waiting for in flight messages")
    await root.stop()
    producer.assert_has_calls(
        [mock.call.ack([i[Block.MSG_ID_FIELD]], [Result(status=Status.SUCCESS, payload=i)]) for i in messages])


@pytest.mark.asyncio
async def test_acks_exception():
    # test failure of a block propagates upward
    root = Step("A", ExceptionBlock(), concurrency=1)
    messages = [
        {Block.MSG_ID_FIELD: "message1", "value": True},
        {Block.MSG_ID_FIELD: "message2", "value": True}
    ]
    producer_mock = mock.MagicMock()
    root.add_done_callback(producer_mock.ack)
    for message in messages:
        await root.process([message])
    await root.stop()
    assert producer_mock.ack.call_args_list == [mock.call.ack(
        [i[Block.MSG_ID_FIELD]], [Result(Status.REJECTED, "Error in step A: ValueError()")]) for i in messages]


@pytest.mark.asyncio
async def test_step_buffer_by_size():
    results_block = mock.Mock(wraps=EchoBlock())
    root = StepBuffer("BUFFER", min_buffer_size=4, flush_ms=100000)
    root | Step("A", results_block, concurrency=1)
    messages = [
        {Block.MSG_ID_FIELD: "message1", "value": True},
        {Block.MSG_ID_FIELD: "message2", "value": True},
        {Block.MSG_ID_FIELD: "message3", "value": True},
        {Block.MSG_ID_FIELD: "message4", "value": True},
    ]
    producer_mock = mock.MagicMock()
    root.add_done_callback(producer_mock.ack)
    for message in messages:
        await root.process([message])
    await root.stop()
    producer_mock.assert_has_calls([
        mock.call.ack(
            [i[Block.MSG_ID_FIELD] for i in messages],
            [Result(status=Status.SUCCESS, payload=i) for i in messages]
        )
    ])


@pytest.mark.asyncio
async def test_step_buffer_by_timeout():
    results_block = mock.Mock(wraps=EchoBlock())
    root = StepBuffer("BUFFER", min_buffer_size=4, flush_ms=500)
    root | Step("A", results_block, concurrency=1)
    messages = [
        {Block.MSG_ID_FIELD: "message1", "value": True},
        {Block.MSG_ID_FIELD: "message2", "value": True},
        {Block.MSG_ID_FIELD: "message3", "value": True},
    ]
    producer_mock = mock.MagicMock()
    root.add_done_callback(producer_mock.ack)
    for message in messages:
        await root.process([message])
    await asyncio.sleep(0.5)
    await root.stop()
    producer_mock.assert_has_calls([
        mock.call.ack(
            [i[Block.MSG_ID_FIELD] for i in messages],
            [Result(status=Status.SUCCESS, payload=i) for i in messages]
        )
    ])
