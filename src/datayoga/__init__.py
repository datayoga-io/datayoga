import logging
from typing import Any, Dict, List

from datayoga.job import Job

logger = logging.getLogger(__name__)


def compile(job_yaml: Dict[str, Any]) -> Job:
    logger.info("**** INSIDE compile ****")
    job = Job(job_yaml)
    return job


def transform(job: Job, data: List[Dict[str, Any]], context: Any = None) -> List[Dict[str, Any]]:
    return job.transform(data, context)
