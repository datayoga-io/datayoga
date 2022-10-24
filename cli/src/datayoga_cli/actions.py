import logging
from typing import List
from datayoga.step import Step
from tqdm import tqdm
from datayoga.job import Job

logger = logging.getLogger("dy")


async def run(job: Job):
    # create the step sequence
    root = None
    for block in job.blocks:
        if root is None:
            root = Step("A", block)
            last_step = root
        else:
            last_step = last_step.append(Step("B", block))

    root.add_done_callback(lambda msg_ids, result, reason: job.input.ack(msg_ids))

    for record in tqdm(job.input.produce()):
        logger.debug(f"Retrieved record:\n\t{record}")
        await root.process([record])

    # wait for in-flight records to finish
    await root.join()

    # graceful shutdown
    await root.stop()
