import asyncio
import time
from typing import Callable


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
                print(f"worker {id} is running: {not worker.done()}")

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
        print(f"{self.id} enqueued {i}")

    async def run(self, worker_id):
        while True:
            entry = await self.queue.get()
            try:
                print(f"{self.id}-{worker_id} processing {entry[0]['msg_id']}")
                await self.block.run([i["value"] for i in entry])
            finally:
                # TODO: add error handling
                if self.child:
                    await self.child.process(entry)
                    print(f"{self.id} child done enqueue {entry[0]['msg_id']}")
                else:
                    # we are a last channel, propagate the ack upstream
                    self.done([x["msg_id"] for x in entry])
                self.queue.task_done()
            print(f"{self.id}-{worker_id} done processing {entry[0]['msg_id']}")

    def done(self, msg_id):
        print(f"{self.id} acking {msg_id}")
        self.active_entries.difference_update(msg_id)
        if self.done_callback:
            self.done_callback(msg_id)

    async def join(self):
        # wait for all active entries to be processed
        while len(self.active_entries) > 0:
            # print(self.active_entries)
            await asyncio.sleep(0.1)

    async def stop(self):
        # wait for all tasks to finish
        await self.join()

        # start by stopped children workers
        if self.child:
            await self.child.stop()

        # stop all workers in the pool
        for worker in self.workers:
            worker.cancel()
