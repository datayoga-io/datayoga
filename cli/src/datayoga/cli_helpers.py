import logging
import sys
import traceback
from logging import Formatter, Logger
from typing import Callable, Sequence

import jsonschema
import yaml


class CustomFormatter(Formatter):
    class Bcolors:
        HEADER = "\033[95m"
        GREY = "\x1b[38;20m"
        BLUE = "\033[94m"
        GREEN = "\033[92m"
        YELLOW = "\033[93m"
        RED = "\033[91m"
        ENDC = "\033[0m"
        BOLD = "\033[1m"
        UNDERLINE = "\033[4m"
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


def add_options(options: Sequence[Callable]):
    def _add_options(func):
        for option in reversed(options):
            func = option(func)
        return func

    return _add_options


def handle_critical(logger: Logger, msg: str, e: Exception):
    logger.critical(f"{msg}:\n{e}")
    if logger and logger.isEnabledFor(logging.DEBUG):
        traceback.print_exc()

    sys.exit(1)


def pprint_yaml_validation_error(
        job_filename: str, schema_error: jsonschema.exceptions.ValidationError, logger: Logger, show_lines_before: int = 4,
        show_lines_after: int = 2):
    with open(job_filename, "r", encoding="utf8") as job_file:
        job_source = job_file.read()

    if schema_error.absolute_path:
        # get the line number in the YAML data corresponding to the error
        line_number = _find_line_number_by_path(schema_error.absolute_path, job_source)
        logger.error(f"failed validating {job_filename}")
        logger.error(f"line: {line_number+1}, {schema_error.message}")
        # print out lines of code
        lines = job_source.split("\n")
        err_lines = lines[max(line_number-show_lines_before, 0):line_number]
        # add the error line itself
        err_lines.append(CustomFormatter.Bcolors.BOLD + lines[line_number] + CustomFormatter.Bcolors.RESET)
        err_line_indent = len(lines[line_number]) - len(lines[line_number].lstrip())
        err_lines.append(" " * err_line_indent + "^")
        err_lines.extend(lines[line_number + 1: min(line_number + 1 + show_lines_after, len(lines))])
        print("\n".join(err_lines))
    else:
        handle_critical(logger, f"{schema_error}", schema_error)


# helper methods for yaml validation message method
def _find_in_level(location, node):
    if isinstance(location, int):
        # we are searching in a list
        return node.value[location]

    # we need to traverse to find the key
    for entry in node.value:
        if entry[0].value == location:
            return entry[1]

    raise ValueError(f"cannot find yaml path {location}")


def _find_line_number_by_path(path, yaml_input):
    node = yaml.compose(yaml_input)
    for item in path:
        node = _find_in_level(item, node)

    return node.start_mark.line
