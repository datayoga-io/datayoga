import asyncio
import logging

from datayoga_core.step import Step

logger = logging.getLogger("dy")


class StepBuffer(Step):
    def __init__(self, step_id: str, min_buffer_size=4, max_buffer_size=4, flush_ms=1000):
        super().__init__(step_id, None)
        self.min_buffer_size = min_buffer_size
        self.max_buffer_size = max(max_buffer_size, min_buffer_size)
        self.buffer = []
        self.flush_ms = flush_ms
        self.timer = None
        self.concurrency_lock = asyncio.Semaphore(1)

    async def flush_timer(self):
        await asyncio.sleep(self.flush_ms/1000)
        logger.debug("flushing on timeout")
        await self.flush()

    async def run(self, worker_id: int):
        while True:
            entry = await self.queue.get()
            logger.debug(f"appending {entry}")
            if (self.timer is None or self.timer.cancelled()) and self.flush_ms is not None:
                # first record, we add a timer
                logger.debug("creating timer")
                self.timer = asyncio.create_task(self.flush_timer())
            else:
                logger.debug("no timer")
            self.buffer.extend(entry)

            if len(self.buffer) >= self.min_buffer_size:
                logger.debug(f"flushing on buffer size {self.buffer} {len(self.buffer)}")
                if self.timer:
                    self.timer.cancel()
                await self.flush()

    async def flush(self):
        # flush buffer
        await self.concurrency_lock.acquire()
        try:
            # we may have accumulated a larger buffer while we flushed, so flush in max_buffer_size batches
            while len(self.buffer) > 0:
                logging.debug(f"flushing {self.buffer}")
                # check if we have a next step
                if self.next_step:
                    # process downstream
                    logging.debug(f"sending to next step")
                    await self.next_step.process([self.buffer.pop(0) for _ in range(min(len(self.buffer), self.max_buffer_size))])
        finally:
            self.concurrency_lock.release()
