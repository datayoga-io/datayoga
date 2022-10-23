import logging
from typing import Any, Dict, List, Optional, TypedDict

from datayoga.context import Context
from datayoga.job import Job

logger = logging.getLogger("dy")


def compile(
        job_settings: Dict[str, Any],
        context: Optional[Context] = None, whitelisted_blocks: Optional[List[str]] = None) -> Job:
    """
    Compiles a job in YAML 

    Args:
        job_settings (Dict[str, Any]): Job settings
        context (Optional[Context], optional): Context. Defaults to None.
        whitelisted_blocks: (Optional[List[str]], optional): Whitelisted blocks. Defaults to None.

    Returns:
        Job: Compiled job
    """
    logger.debug("Compiling job")
    return Job(job_settings, context, whitelisted_blocks)


def validate(job_settings: Dict[str, Any], whitelisted_blocks: Optional[List[str]] = None):
    """
    Validates a job in YAML 

    Args:
        job_settings (Dict[str, Any]): Job settings
        whitelisted_blocks: (Optional[List[str]], optional): Whitelisted blocks. Defaults to None.

    Raises:
        ValueError: when the job is invalid
    """
    logger.debug("Validating job")
    try:
        Job(job_settings, whitelisted_blocks=whitelisted_blocks)
    except Exception as e:
        raise ValueError(e)


def transform(job_settings: Dict[str, Any],
              data: List[Dict[str, Any]],
              context: Optional[Context] = None, whitelisted_blocks: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """
    Transforms data against a certain job

    Args:
        job_settings (Dict[str, Any]): Job settings
        data (List[Dict[str, Any]]): Data to transform
        context (Optional[Context], optional): Context. Defaults to None.
        whitelisted_blocks: (Optional[List[str]], optional): Whitelisted blocks. Defaults to None.

    Returns:
        List[Dict[str, Any]]: Transformed data
    """
    job = compile(job_settings, context, whitelisted_blocks)
    logger.debug("Transforming data")
    return job.transform(data)
