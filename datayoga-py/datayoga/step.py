import asyncio
import logging
from typing import Callable, List, Optional
from enum import Enum     # for enum34, or the stdlib version
from datayoga.block import Block, Result

logger = logging.getLogger("dy")


class Step():
    def __init__(self, id, block, concurrency=1):
        self.id = id
        self.block = block
        self.next_step = None
        self.queue = asyncio.Queue(maxsize=1)
        self.active_entries = set()
        self.concurrency = concurrency
        self.workers = [None]*self.concurrency
        self.done_callback = None

    def init(self):
        # initialize the block
        self.block.init()
        # start pool of workers for parallelization
        self.start_pool()

    def start_pool(self):
        for id in range(self.concurrency):
            worker = self.workers[id]
            if worker is None or not worker.done():
                self.workers[id] = asyncio.create_task(self.run(id))
            else:
                logger.debug(f"worker {id} is running: {not worker.done()}")

    def add_done_callback(self, callback: Callable[[str], None]):
        self.done_callback = callback

    def __or__(self, other):
        return self.append(other)

    def append(self, next_step):
        self.next_step = next_step
        self.next_step.add_done_callback(self.done)
        return self.next_step

    async def process(self, i):
        self.active_entries.update([x[Block.MSG_ID_FIELD] for x in i])
        await self.queue.put(i)

    async def run(self, worker_id):
        while True:
            entry = await self.queue.get()
            logger.debug(f"{self.id}-{worker_id} processing {[i[Block.MSG_ID_FIELD] for i in entry]}")
            try:
                processed_entries, results = await self.block.run(entry)

                # TODO: handle filtered. anything not processed or rejected
                # check if we have a next step
                if self.next_step:
                    # process downstream
                    await self.next_step.process(processed_entries)
                else:
                    # we are a last channel, propagate the ack upstream
                    # TODO: verify that all entries have a msg id otherwise raise consistency error
                    self.done([x[Block.MSG_ID_FIELD] for x in processed_entries], Result.SUCCESS)

                    # indicate rejected records if any
                    if Result.REJECTED in results:
                        self.done([entry[i][Block.MSG_ID_FIELD]
                                   for i, v in enumerate(results) if v == Result.REJECTED], Result.REJECTED)
            except Exception as e:
                # we caught an exception. the entire batch is considered rejected
                logger.exception(e)
                # verify that all messages still have a msg_id property
                # if next(filter(lambda x: not Block.MSG_ID_FIELD in x,entry),None) is not None
                self.done([x[Block.MSG_ID_FIELD] for x in entry],
                          Result.REJECTED, f"Error in step {self.id}: {repr(e)}")
            finally:
                self.queue.task_done()
            logger.debug(f"{self.id}-{worker_id} done processing {entry[0][Block.MSG_ID_FIELD]}")

    def done(self, msg_ids: List[str], result: Optional[Result] = None, reason: Optional[str] = None):
        logger.debug(f"{self.id} acking {msg_ids} with result {result}")
        self.active_entries.difference_update(msg_ids)
        if self.done_callback is not None:
            self.done_callback(msg_ids, result, reason)

    async def join(self):
        # wait for all active entries to be processed
        while len(self.active_entries) > 0:
            await asyncio.sleep(0.2)

    async def stop(self):
        # wait for all tasks to finish
        await self.join()

        # stop any downstream workers
        if self.next_step:
            await self.next_step.stop()

        # stop all workers in the pool
        for worker in self.workers:
            worker.cancel()
