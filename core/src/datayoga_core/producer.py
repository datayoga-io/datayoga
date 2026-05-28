import asyncio
import logging
from contextlib import suppress
from typing import Any, AsyncGenerator, Dict, List

from .block import Block

logger = logging.getLogger("dy")


class Message:
    def __init__(self, msg_id: str, value: Dict[str, Any]):
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
        """Re-chunks `produce_chunks()` output to exact batch_size batches.

        Reads `batch_size` and `flush_ms` from properties lazily so subclasses
        don't need to remember to call `super().init()`.
        """
        batch_size = int(self.properties.get("batch_size", self.DEFAULT_BATCH_SIZE))
        flush_ms = self.properties.get("flush_ms", self.DEFAULT_FLUSH_MS)
        timeout = (flush_ms / 1000) if flush_ms else None

        queue: asyncio.Queue = asyncio.Queue()
        EOS = object()

        async def pump():
            try:
                async for chunk in self.produce_chunks():
                    if chunk:
                        await queue.put(chunk)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("produce_chunks raised; ending stream: %s", exc)
            finally:
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
