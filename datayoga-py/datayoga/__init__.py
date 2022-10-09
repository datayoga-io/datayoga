import logging
from typing import Any, Dict, List, Optional

from datayoga.context import Context
from datayoga.job import Job

logger = logging.getLogger("dy")


def compile(job_settings: Dict[str, Any], context: Optional[Context] = None) -> Job:
    """
    Compiles a job in YAML 

    Args:
        job_settings (Dict[str, Any]): Job settings
        context (Optional[Context], optional): Context. Defaults to None.

    Returns:
        Job: Compiled job
    """
    logger.debug("Compiling job")
    return Job(job_settings, context)


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
              context: Optional[Context] = None) -> List[Dict[str, Any]]:
    """
    Transforms data against a certain job

    Args:
        job_settings (Dict[str, Any]): Job settings
        data (List[Dict[str, Any]]): Data to transform
        context (Optional[Context], optional): Context. Defaults to None.

    Returns:
        List[Dict[str, Any]]: Transformed data
    """
    job = compile(job_settings, context)
    logger.debug("Transforming data")
    return job.transform(data)
