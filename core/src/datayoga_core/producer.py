import asyncio
import logging
from contextlib import suppress
from typing import Any, AsyncGenerator, Dict, List

from .block import Block

logger = logging.getLogger("dy")


class Message:
    """A message produced by a producer block."""

    def __init__(self, msg_id: str, value: Dict[str, Any]):
        """Initializes a message with an id and a payload value."""
        self.msg_id = msg_id
        self.value = value


class Producer(Block):
    """Base class for producer (read) blocks.

    Subclasses override `produce_chunks()` to yield chunks of any size from
    the source. The default `produce()` re-chunks them to exactly `batch_size`
    records per batch (smaller on flush_ms timeout or end-of-stream).

    Legacy subclasses may still override `produce()` directly. They bypass
    the base-class batching and `produce_chunks` is not called.
    """

    DEFAULT_BATCH_SIZE = 1000
    DEFAULT_FLUSH_MS = None  # streaming subclasses override to enable timeout flush

    async def produce_chunks(self) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """Yield natural-size chunks from the source.

        Subclasses should override this method. The base-class `produce()`
        will re-chunk the output to exact `batch_size` slices.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must override produce_chunks() or produce()"
        )
        # Make this an async generator for type-checking purposes.
        yield  # pragma: no cover

    async def produce(self) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """Re-chunks `produce_chunks()` output into batches of up to `batch_size`.

        Each batch is exactly `batch_size` except for the last batch on
        end-of-stream and any partial batch flushed by `flush_ms` inactivity.

        Reads `batch_size` and `flush_ms` from properties lazily so subclasses
        don't need to remember to call `super().init()`.

        Source errors raised by `produce_chunks()` propagate to the caller (the
        job aborts) rather than being treated as a silent end-of-stream. The
        background pump uses a bounded queue so source reads cannot outpace
        downstream consumption — the existing backpressure is preserved.
        """
        batch_size = int(self.properties.get("batch_size", self.DEFAULT_BATCH_SIZE))
        flush_ms = self.properties.get("flush_ms", self.DEFAULT_FLUSH_MS)
        timeout = (flush_ms / 1000) if flush_ms else None

        # maxsize=1 keeps the pump exactly one chunk ahead of the consumer,
        # which restores the natural backpressure the old yield-driven model had.
        queue: asyncio.Queue = asyncio.Queue(maxsize=1)
        EOS = object()
        pump_error: List[BaseException] = []  # length 0 or 1

        async def pump():
            """Drains produce_chunks() into the queue; signals EOS on exit and captures errors."""
            cancelled = False
            try:
                async for chunk in self.produce_chunks():
                    if chunk:
                        await queue.put(chunk)
            except asyncio.CancelledError:
                cancelled = True
                raise
            except BaseException as exc:
                pump_error.append(exc)
            finally:
                # Skip the EOS put when cancelled — the consumer's finally is
                # awaiting us, the queue may be full (maxsize=1), and putting
                # would deadlock. The consumer won't read EOS anyway.
                if not cancelled:
                    await queue.put(EOS)

        pump_task = asyncio.create_task(pump())
        buffer: List[Dict[str, Any]] = []
        try:
            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=timeout)
                except asyncio.TimeoutError:
                    if buffer:
                        yield buffer
                        buffer = []
                    continue

                if item is EOS:
                    if buffer:
                        yield buffer
                    if pump_error:
                        # Re-raise the source error so the job fails loudly
                        # instead of treating a truncated read as success.
                        raise pump_error[0]
                    return

                buffer.extend(item)
                while len(buffer) >= batch_size:
                    yield buffer[:batch_size]
                    buffer = buffer[batch_size:]
        finally:
            pump_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await pump_task

    def ack(self, msg_ids: List[str]):
        """Sends acknowledge for the message IDs of records that have been processed."""
        pass
