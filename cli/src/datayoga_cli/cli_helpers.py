import logging
import sys
from logging import Formatter, Logger
from typing import Any, Dict, List, Union

# set up logging


class CustomFormatter(Formatter):
    class Bcolors:
        HEADER = '\033[95m'
        GREY = "\x1b[38;20m"
        BLUE = '\033[94m'
        GREEN = '\033[92m'
        YELLOW = '\033[93m'
        RED = '\033[91m'
        ENDC = '\033[0m'
        BOLD = '\033[1m'
        UNDERLINE = '\033[4m'
        RESET = "\x1b[0m"

    format = "%(levelname)s - %(message)s"

    FORMATS = {
        logging.DEBUG: Bcolors.GREY + format + Bcolors.RESET,
        logging.INFO: Bcolors.GREY + format + Bcolors.RESET,
        logging.WARNING: Bcolors.YELLOW + format + Bcolors.RESET,
        logging.ERROR: Bcolors.RED + format + Bcolors.RESET,
        logging.CRITICAL: Bcolors.RED + Bcolors.BOLD + format + Bcolors.RESET
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def add_options(options: List[str]):
    def _add_options(func):
        for option in reversed(options):
            func = option(func)
        return func

    return _add_options
