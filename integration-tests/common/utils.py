
import logging
import os
import time
from os import path
from subprocess import PIPE, Popen
from typing import Optional

logger = logging.getLogger("dy")


def execute_program(command: str):
    """Executes a child program in a new process and logs its output.

    Args:
        command (str): script or command to run.

    Raises:
        ValueError: When the return code is not 0.
    """
    process = Popen(command, stdin=PIPE, stdout=PIPE, shell=True, universal_newlines=True)
    while process.poll() is None:
        logger.info(process.stdout.readline().rstrip())
        time.sleep(0.2)

    process.communicate()[0]
    if process.returncode != 0:
        raise ValueError(f"command {command} failed")


def run_job(job: str, piped_from: Optional[str] = None, piped_to: Optional[str] = None):
    piped_from_cmd = f"{piped_from} | " if piped_from else ""
    piped_to_cmd = f" > {piped_to}" if piped_to else ""

    execute_program(
        f'{piped_from_cmd}datayoga run {job} --dir {path.join(os.path.dirname(os.path.realpath(__file__)), "..", "resources")} --loglevel DEBUG{piped_to_cmd}')
