import logging
from typing import Any, Dict

from datayoga.context import Context
from datayoga.job import Job

logger = logging.getLogger(__name__)


def compile(job_yaml: Dict[str, Any]) -> Job:
    logger.info("**** INSIDE compile ****")
    job = Job(job_yaml)
    return job


def transform(job: Job, data: Any, context: Context = None) -> Any:
    return job.transform(data, context)
