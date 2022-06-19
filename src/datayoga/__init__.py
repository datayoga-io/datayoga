import logging
from typing import Any, Dict

from datayoga.context import Context
from datayoga.job import Job

logger = logging.getLogger(__name__)


def compile(job_yaml: Dict[str, Any]) -> Job:
    """
    Compiles a job in YAML 

    Args:
        job_yaml (Dict[str, Any]): Job in YAML format

    Returns:
        Job: Compiled job
    """
    logger.debug("Compiling job")
    job = Job(job_yaml)
    return job


def transform(job: Job, data: Any, context: Context = None) -> Any:
    """
    Transforms data against a certain job

    Args:
        job (Job): Job to transform against
        data (Any): Data to transform
        context (Context, optional): Context. Defaults to None.

    Returns:
        Any: Transformed data
    """
    logger.debug("Transforming data")
    return job.transform(data, context)
