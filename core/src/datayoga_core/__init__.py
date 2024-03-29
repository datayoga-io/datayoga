import logging
from typing import Any, Dict, List, Optional

from datayoga_core.context import Context
from datayoga_core.job import Job
from datayoga_core.result import JobResult

logger = logging.getLogger("dy")


def compile(job_settings: Dict[str, Any], whitelisted_blocks: Optional[List[str]] = None) -> Job:
    """Compiles a job in YAML.

    Args:
        job_settings (Dict[str, Any]): Job settings.
        whitelisted_blocks: (Optional[List[str]], optional): Whitelisted blocks. Defaults to None.

    Returns:
        Job: Compiled job.
    """
    logger.debug("Compiling job")
    return Job.compile(job_settings, whitelisted_blocks)


def validate(job_settings: Dict[str, Any], whitelisted_blocks: Optional[List[str]] = None):
    """Validates a job in YAML.

    Args:
        job_settings (Dict[str, Any]): Job settings.
        whitelisted_blocks: (Optional[List[str]], optional): Whitelisted blocks. Defaults to None.

    Raises:
        ValueError: When the job is invalid.
    """
    logger.debug("Validating job")
    try:
        Job.validate(
            source=job_settings,
            whitelisted_blocks=whitelisted_blocks
        )
    except Exception as e:
        raise ValueError(e)


def transform(
    job_settings: Dict[str, Any],
    data: List[Dict[str, Any]],
    context: Optional[Context] = None,
    whitelisted_blocks: Optional[List[str]] = None
) -> JobResult:
    """Transforms data against a certain job.

    Args:
        job_settings (Dict[str, Any]): Job settings.
        data (List[Dict[str, Any]]): Data to transform.
        context (Optional[Context]): Context. Defaults to None.
        whitelisted_blocks: (Optional[List[str]]): Whitelisted blocks. Defaults to None.

    Returns:
        JobResult: Job result.
    """
    job = compile(job_settings, whitelisted_blocks)
    job.init(context)
    logger.debug("Transforming data")
    return job.transform(data)
