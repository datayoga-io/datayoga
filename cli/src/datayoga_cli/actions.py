import logging
from typing import List
from datayoga.step import Step
from tqdm import tqdm
from datayoga.job import Job

logger = logging.getLogger("dy")


async def run(job: Job):
    await job.run()
