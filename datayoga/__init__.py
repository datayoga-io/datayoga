import logging
from typing import Any, Dict, List

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
    return Job(job_settings)


def validate(job_settings: Dict[str, Any]):
    """
    Validates a job in YAML 

    Args:
        job_settings (Dict[str, Any]): Job settings

    Raises:
        ValueError: when the job is invalid
    """
    logger.debug("Validating job")
    try:
        Job(job_settings)
    except Exception as e:
        raise ValueError(e)


def transform(job_settings: Dict[str, Any],
              data: List[Dict[str, Any]],
              context: Context = None) -> List[Dict[str, Any]]:
    """
    Transforms data against a certain job

    Args:
        job_settings (Dict[str, Any]): Job settings
        data (List[Dict[str, Any]]): Data to transform
        context (Context, optional): Context. Defaults to None.

    Returns:
        List[Dict[str, Any]]: Transformed data
    """
    job = compile(job_settings)
    logger.debug("Transforming data")
    return job.transform(data, context)
