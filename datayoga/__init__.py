import logging
from typing import Any, Dict, List, Tuple

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


def validate(job_settings: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Validates a job in YAML 

    Args:
        job_settings (Dict[str, Any]): Job settings

    Returns:
        Tuple[bool, str]: Indication if the job is valid or not. If not, specify the validation error.
    """
    logger.debug("Validating job")
    is_job_valid = True
    validation_error = None
    try:
        Job(job_settings)
    except Exception as e:
        is_job_valid = False
        validation_error = e

    return (is_job_valid, validation_error)


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
