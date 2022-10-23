import asyncio
import logging
from typing import Callable, List, Optional
from enum import Enum     # for enum34, or the stdlib version

logger = logging.getLogger("dy")

StepResult = Enum('StepResult', 'SUCCESS REJECTED FILTERED')


class Step():
    def __init__(self, id, block, concurrency=1):
        self.id = id
        self.block = block
        self.child = None
        self.queue = asyncio.Queue(maxsize=1)
        self.active_entries = set()
        self.concurrency = concurrency
        self.workers = [None]*self.concurrency
        self.done_callback = None
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
        return self.add_child(other)

    def add_child(self, child):
        self.child = child
        self.child.add_done_callback(self.done)
        return self.child

    async def process(self, i):
        self.active_entries.update([x['msg_id'] for x in i])
        await self.queue.put(i)

    async def run(self, worker_id):
        while True:
            entry = await self.queue.get()
            try:
                msg_ids = [i["msg_id"] for i in entry]
                logger.debug(f"{self.id}-{worker_id} processing {msg_ids}")
                processed_entry = await self.block.run([i["value"] for i in entry])
                # check if we have a next step
                if self.child:
                    # re-combine the values with the msg_ids
                    await self.child.process([{'msg_id': k, 'value': v} for k, v in list(zip(msg_ids, processed_entry))])
                else:
                    # we are a last channel, propagate the ack upstream
                    self.done(msg_ids, StepResult.SUCCESS)
            except Exception as e:
                logger.debug(e)
                self.done([x["msg_id"] for x in entry], StepResult.REJECTED, f"Error in step {self.id}: {repr(e)}")
            finally:
                self.queue.task_done()
            logger.debug(f"{self.id}-{worker_id} done processing {entry[0]['msg_id']}")

    def done(self, msg_ids: List[str], result: Optional[StepResult] = None, reason: Optional[str] = None):
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

        # start by stopped children workers
        if self.child:
            await self.child.stop()

        # stop all workers in the pool
        for worker in self.workers:
            worker.cancel()
