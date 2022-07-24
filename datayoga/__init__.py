import logging
from typing import Any, Dict

from datayoga.context import Context
from datayoga.job import Job

logger = logging.getLogger(__name__)


def compile(job_settings: Dict[str, Any]) -> Job:
    """
    Compiles a job in YAML 

    Args:
        job_settings (Dict[str, Any]): Job settings

    Returns:
        Job: Compiled job
    """
    logger.debug("Compiling job")
    job = Job(job_settings)
    return job


def transform(job_settings: Dict[str, Any], data: Any, context: Context = None) -> Any:
    """
    Transforms data against a certain job

    Args:
        job_settings (Dict[str, Any]): Job settings
        data (Any): Data to transform
        context (Context, optional): Context. Defaults to None.

    Returns:
        Any: Transformed data
    """
    job = compile(job_settings)
    logger.debug("Transforming data")
    return job.transform(data, context)
