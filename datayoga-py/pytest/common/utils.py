
import logging
import os
import time
from os import path
from subprocess import PIPE, Popen
from typing import Any, Dict

from datayoga.utils import read_yaml

logger = logging.getLogger("dy")


def get_job_settings_from_yaml(filename: str) -> Dict[str, Any]:
    job_settings = read_yaml(path.join(os.path.dirname(os.path.realpath(__file__)), "..", "resources", filename))
    logger.debug(f"job_settings: {job_settings}")
    return job_settings


def execute_program(command: str):
    """Executes a child program in a new process and logs its output.

    Args:
        command (str): script or command to run.

    Raises:
        ValueError: When the return code is not 0.
    """
    logger.debug(f"Running command: {command}")
    process = Popen(command, stdin=PIPE, stdout=PIPE, shell=True, universal_newlines=True)
    while process.poll() is None:
        logger.info(process.stdout.readline().rstrip())
        time.sleep(0.2)

    process.communicate()[0]
    if process.returncode != 0:
        raise ValueError(f"command {command} failed")


def run_job(job_file: str):
    execute_program(
        f'datayoga run {path.join(os.path.dirname(os.path.realpath(__file__)), "..", "resources", job_file)} --loglevel DEBUG')
