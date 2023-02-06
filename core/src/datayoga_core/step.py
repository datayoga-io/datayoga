from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional

from datayoga_core.block import Block
from datayoga_core.context import Context
from datayoga_core.result import Result, Status

logger = logging.getLogger("dy")


class Step():
    def __init__(self, id: str, block: Block, concurrency=1):
        self.id = id
        self.block = block
        self.next_step = None
        self.active_entries = set()
        self.concurrency = concurrency
        self.workers = [None]*self.concurrency
        self.done_callback = None
        self.initialized = False

    def init(self, context: Optional[Context] = None):
        # initialize the block
        self.block.init(context)

    async def start_pool(self):
        # start pool of workers for parallelization
        logger.debug("starting pool")
        self.queue = asyncio.Queue(maxsize=1)
        for id in range(self.concurrency):
            worker = self.workers[id]
            if worker is None or not worker.done():
                self.workers[id] = asyncio.create_task(self.run(id))
            else:
                logger.debug(f"worker {id} is running: {not worker.done()}")

    def add_done_callback(self, callback: Callable[[str], None]):
        self.done_callback = callback

    def __or__(self, other: Step):
        return self.append(other)

    def append(self, next_step: Step):
        self.next_step = next_step
        self.next_step.add_done_callback(self.done)
        return self.next_step

    async def process(self, messages: List[Dict[str, Any]]):
        if not self.initialized:
            await self.start_pool()
            self.initialized = True
        self.active_entries.update([x[Block.MSG_ID_FIELD] for x in messages])
        await self.queue.put(messages)

    async def run(self, worker_id: int):
        while True:
            entry = await self.queue.get()
            logger.debug(f"{self.id}-{worker_id} processing {[i[Block.MSG_ID_FIELD] for i in entry]}")
            try:
                processed_entries, filtered_entries, rejected_entries = await self.block.run(entry)

                # handle filtered. anything not processed or rejected
                logger.debug(
                    f"filtered entries: {filtered_entries}, processed entries: {processed_entries}, rejected entries: {rejected_entries}")
                if filtered_entries:
                    # ack the filtered entries
                    self.done([row.payload[Block.MSG_ID_FIELD] for row in filtered_entries], [Result(Status.FILTERED)] * len(filtered_entries))

                # handle rejected
                if rejected_entries:
                    # ack the rejected entries
                    self.done([row.payload[Block.MSG_ID_FIELD] for row in rejected_entries], rejected_entries)

                if processed_entries:
                    # check if we have a next step
                    if self.next_step:
                        # process downstream
                        await self.next_step.process([result.payload for result in processed_entries])
                    else:
                        # we are a last channel, propagate the ack upstream
                        # TODO: verify that all entries have a msg id otherwise raise consistency error
                        self.done([row.payload[Block.MSG_ID_FIELD] for row in processed_entries], processed_entries)

            except Exception as e:
                # we caught an exception. the entire batch is considered rejected
                logger.exception(e)
                # verify that all messages still have a msg_id property
                # if next(filter(lambda x: not Block.MSG_ID_FIELD in x,entry),None) is not None
                self.done([x[Block.MSG_ID_FIELD] for x in entry], [
                          Result(Status.REJECTED, f"Error in step {self.id}: {repr(e)}")] * len(entry))
            finally:
                self.queue.task_done()
            logger.debug(f"{self.id}-{worker_id} done processing {entry}")

    def done(self, msg_ids: List[str], results: List[Result]):
        logger.debug(f"{self.id} acking {msg_ids}")
        self.active_entries.difference_update(msg_ids)
        if self.done_callback is not None:
            self.done_callback(msg_ids, results)

    async def join(self):
        # wait for all active entries to be processed
        while len(self.active_entries) > 0:
            logger.debug(f"{self.id} waiting for dangling messages: {self.active_entries}")
            await asyncio.sleep(0.2)

    async def stop(self):
        # wait for all tasks to finish
        await self.join()
        logger.debug("joined")

        # stop the block
        if self.block:
            self.block.stop()

        # stop any downstream workers
        if self.next_step:
            await self.next_step.stop()

        # stop all workers in the pool
        for worker in self.workers:
            if worker:
                worker.cancel()
