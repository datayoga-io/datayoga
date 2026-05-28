import asyncio
from typing import AsyncGenerator, List, Optional

import pytest
from datayoga_core.context import Context
from datayoga_core.producer import Message, Producer


def _msg(i: int) -> dict:
    """Builds a record carrying the producer MSG_ID_FIELD and a numeric value."""
    return {Producer.MSG_ID_FIELD: str(i), "v": i}


class FakeProducer(Producer):
    """Producer driven by a scripted list of chunks plus optional sleeps."""

    def __init__(self, properties=None, *, chunks=None, sleep_before=None):
        """Configures the scripted chunks and optional per-chunk sleep delays."""
        # schema for a FakeProducer; declare batch_size/flush_ms so validation passes
        self._test_schema = {
            "type": "object",
            "properties": {
                "batch_size": {"type": "integer", "minimum": 1},
                "flush_ms": {"type": ["integer", "null"], "minimum": 1},
            },
        }
        self._chunks = chunks or []
        self._sleep_before = sleep_before or []
        super().__init__(properties or {})

    def get_json_schema(self):
        """Returns the in-memory test schema (avoids reading from disk)."""
        return self._test_schema

    def init(self, context: Optional[Context] = None):
        """No-op init; FakeProducer doesn't need any setup."""
        pass

    async def produce_chunks(self) -> AsyncGenerator[List[Message], None]:
        """Yields the scripted chunks, optionally sleeping before each one."""
        for i, chunk in enumerate(self._chunks):
            if i < len(self._sleep_before) and self._sleep_before[i]:
                await asyncio.sleep(self._sleep_before[i])
            yield chunk


async def _drain(producer: Producer):
    """Collects all batches emitted by a producer until end-of-stream."""
    out = []
    async for batch in producer.produce():
        out.append(batch)
    return out


@pytest.mark.asyncio
async def test_rechunks_one_large_chunk():
    chunks = [[_msg(i) for i in range(5000)]]
    p = FakeProducer({"batch_size": 1000}, chunks=chunks)
    batches = await _drain(p)
    assert [len(b) for b in batches] == [1000, 1000, 1000, 1000, 1000]


@pytest.mark.asyncio
async def test_accumulates_small_chunks_and_flushes_on_eos():
    chunks = [[_msg(i) for i in range(200)],
              [_msg(i) for i in range(200, 500)],
              [_msg(i) for i in range(500, 900)]]
    p = FakeProducer({"batch_size": 1000}, chunks=chunks)
    batches = await _drain(p)
    assert [len(b) for b in batches] == [900]


@pytest.mark.asyncio
async def test_partial_final_batch_on_eos():
    chunks = [[_msg(i) for i in range(1500)]]
    p = FakeProducer({"batch_size": 1000}, chunks=chunks)
    batches = await _drain(p)
    assert [len(b) for b in batches] == [1000, 500]


@pytest.mark.asyncio
async def test_empty_chunks_are_ignored():
    chunks = [[], [_msg(1), _msg(2)], [], [_msg(3)]]
    p = FakeProducer({"batch_size": 10}, chunks=chunks)
    batches = await _drain(p)
    assert [len(b) for b in batches] == [3]


@pytest.mark.asyncio
async def test_flush_ms_emits_partial_on_inactivity():
    # one chunk of 2 records, then a 300ms wait before EOS; flush_ms=100 should
    # flush the partial batch of 2 well before EOS.
    chunks = [[_msg(1), _msg(2)], [_msg(3)]]
    sleeps = [0, 0.3]
    p = FakeProducer({"batch_size": 100, "flush_ms": 100},
                     chunks=chunks, sleep_before=sleeps)

    received = []
    started = asyncio.get_event_loop().time()
    timings = []
    async for batch in p.produce():
        timings.append(asyncio.get_event_loop().time() - started)
        received.append(batch)

    assert [len(b) for b in received] == [2, 1]
    # first flush happens because of inactivity (~100ms), not waiting for chunk 2
    assert timings[0] < 0.25, f"expected first flush before 250ms, got {timings[0]}"


@pytest.mark.asyncio
async def test_no_flush_ms_holds_records_until_eos():
    chunks = [[_msg(1)], [_msg(2)]]
    sleeps = [0, 0.1]
    p = FakeProducer({"batch_size": 100}, chunks=chunks, sleep_before=sleeps)
    batches = await _drain(p)
    assert [len(b) for b in batches] == [2]  # combined on EOS, never flushed mid-stream


@pytest.mark.asyncio
async def test_consumer_cancellation_cleans_up_pump():
    chunks = [[_msg(i)] for i in range(1000)]
    p = FakeProducer({"batch_size": 10, "flush_ms": 50}, chunks=chunks,
                     sleep_before=[0.05] * 1000)

    gen = p.produce()
    first = await gen.__anext__()
    assert len(first) >= 1
    await gen.aclose()
    # If pump task wasn't cleaned up we'd see a "Task was destroyed but it is
    # pending!" warning here. Sleep briefly so the loop has a chance to surface it.
    await asyncio.sleep(0.1)


class _BoomProducer(Producer):
    """Producer whose produce_chunks() raises after emitting some chunks."""

    def __init__(self, properties, *, before_error, error):
        """Configures how many chunks to emit before raising."""
        self._test_schema = {
            "type": "object",
            "properties": {"batch_size": {"type": "integer", "minimum": 1}},
        }
        self._before_error = before_error
        self._error = error
        super().__init__(properties)

    def get_json_schema(self):
        """Returns the in-memory test schema (avoids reading from disk)."""
        return self._test_schema

    def init(self, context: Optional[Context] = None):
        """No-op init; _BoomProducer doesn't need any setup."""
        pass

    async def produce_chunks(self) -> AsyncGenerator[List[Message], None]:
        """Emits the scripted lead-in chunks, then raises the configured exception."""
        for chunk in self._before_error:
            yield chunk
        raise self._error


@pytest.mark.asyncio
async def test_source_errors_propagate_instead_of_silent_eos():
    """A failing source must abort the consumer, not look like clean EOS."""
    p = _BoomProducer(
        {"batch_size": 100},
        before_error=[[_msg(1), _msg(2)]],
        error=RuntimeError("source connection lost"),
    )
    with pytest.raises(RuntimeError, match="source connection lost"):
        async for _ in p.produce():
            pass


@pytest.mark.asyncio
async def test_source_error_flushes_buffer_before_raising():
    """Partial buffer is yielded before the error propagates, so already-read
    records aren't dropped on top of the error."""
    p = _BoomProducer(
        {"batch_size": 1000},
        before_error=[[_msg(1), _msg(2), _msg(3)]],
        error=RuntimeError("disk read failed"),
    )
    received = []
    with pytest.raises(RuntimeError, match="disk read failed"):
        async for batch in p.produce():
            received.append(batch)
    assert [len(b) for b in received] == [3]


class _CountingProducer(Producer):
    """Producer that records how many chunks it has been allowed to emit.

    Used to prove the base class applies backpressure (the pump stays no more
    than one chunk ahead of the consumer when maxsize=1).
    """

    def __init__(self, properties, *, num_chunks, chunk_size, on_emit):
        """Configures how many fixed-size chunks to emit and a per-emit hook."""
        self._test_schema = {
            "type": "object",
            "properties": {"batch_size": {"type": "integer", "minimum": 1}},
        }
        self._num_chunks = num_chunks
        self._chunk_size = chunk_size
        self._on_emit = on_emit
        super().__init__(properties)

    def get_json_schema(self):
        """Returns the in-memory test schema (avoids reading from disk)."""
        return self._test_schema

    def init(self, context: Optional[Context] = None):
        """No-op init; _CountingProducer doesn't need any setup."""
        pass

    async def produce_chunks(self) -> AsyncGenerator[List[Message], None]:
        """Yields num_chunks fixed-size chunks, calling on_emit after each yield."""
        for i in range(self._num_chunks):
            yield [_msg(i * self._chunk_size + j) for j in range(self._chunk_size)]
            self._on_emit(i + 1)


@pytest.mark.asyncio
async def test_pump_does_not_outrun_consumer_unboundedly():
    """With the default bounded queue, the pump stays close to the consumer.

    Without backpressure, the pump would emit all 1000 chunks before the
    consumer reads any. With maxsize=1 the pump can be at most ~2 chunks
    ahead at any moment (one being put, one queued).
    """
    emitted_count = [0]

    def record_emit(n):
        emitted_count[0] = n

    p = _CountingProducer(
        {"batch_size": 100},
        num_chunks=1000,
        chunk_size=100,
        on_emit=record_emit,
    )

    gen = p.produce()
    # Pull one batch and observe how far ahead the pump got.
    await gen.__anext__()
    # Yield once so the pump gets a chance to advance after the consumer
    # took one chunk off the queue.
    await asyncio.sleep(0)
    ahead = emitted_count[0]
    await gen.aclose()
    # Pump should be at most a handful of chunks ahead, not all 1000.
    assert ahead <= 5, f"pump emitted {ahead} chunks while consumer pulled 1"
