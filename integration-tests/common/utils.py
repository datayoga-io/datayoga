import logging
import os
import signal
import time
from os import path
from subprocess import PIPE, Popen
from typing import Optional

logger = logging.getLogger("dy")


def execute_program(command: str, background: bool = False) -> Optional[Popen]:
    """Executes a child program in a new process and logs its output.

    Args:
        command (str): script or command to run.
        background (bool): whether to run the command in the background.

    Raises:
        ValueError: When the return code is not 0.
    """
    process = Popen(command, stdin=PIPE, stdout=PIPE, shell=True, universal_newlines=True)

    logger.info(f"Executing {command}...")

    if background:
        return process

    kill_program(process, None)


def kill_program(process: Popen, sig: Optional[int] = signal.CTRL_C_EVENT, igmore_errors: bool = False):
    """Kills a process and logs its output.

    Args:
        process (Popen): process to kill.
        sig (Optional[int]): signal to send to the process.

    Raises:
        ValueError: When the return code is not 0.
    """
    if sig:
        process.send_signal(sig)

    while process.poll() is None:
        logger.info(process.stdout.readline().rstrip())
        time.sleep(0.2)

    process.communicate()

    if not igmore_errors:
        if process.returncode != 0:
            raise ValueError(f"command failed")


def run_job(job: str, piped_from: Optional[str] = None, piped_to: Optional[str] = None,
            background: bool = False) -> Optional[Popen]:
    piped_from_cmd = f"{piped_from} | " if piped_from else ""
    piped_to_cmd = f" > {piped_to}" if piped_to else ""

    command = f'{piped_from_cmd}datayoga run {job} ' \
              f'--dir {path.join(os.path.dirname(os.path.realpath(__file__)), "..", "resources")} ' \
              f'--loglevel DEBUG{piped_to_cmd}'
    return execute_program(command, background=background)
