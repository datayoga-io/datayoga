
import logging
import os
from os import path
from typing import Any, Dict

from datayoga.utils import read_yaml

logger = logging.getLogger("dy")


def get_job_settings_from_yaml(filename: str) -> Dict[str, Any]:
    job_settings = read_yaml(path.join(os.path.dirname(os.path.realpath(__file__)), "..", "resources", filename))
    logger.debug(f"job_settings: {job_settings}")
    return job_settings
