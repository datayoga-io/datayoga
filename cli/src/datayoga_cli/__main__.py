#!/usr/bin/env python
import logging
import cli_helpers
import click
from click.exceptions import UsageError
from jinja2 import Template
from packaging import version
from pkg_resources import get_distribution

LOG_LEVEL_OPTION = [click.option(
    "--loglevel", '-log-level', type=click.Choice(
        ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"],
        case_sensitive=False),
    default="INFO", show_default=True)]

logger = logging.getLogger("cli")

# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setFormatter(cli_helpers.CustomFormatter())
logger.addHandler(ch)


@click.group(name="datayoga", help="DataYoga command line tool")
@click.version_option(get_distribution("datayoga_cli").version)
def cli():
    pass


@cli.command(name="init", help="scaffold a new folder with sample configuration files")
@cli_helpers.add_options(LOG_LEVEL_OPTION)
def init(
    project_name: str,
    loglevel: str
):
    set_logging_level(loglevel)


@cli.command(name="validate", help="Validate job in dry run mode")
@cli_helpers.add_options(LOG_LEVEL_OPTION)
def validate(
        loglevel: str
):
    set_logging_level(loglevel)


@cli.command(name="run", help="Runs a job using a local or remote runner")
@cli_helpers.add_options(LOG_LEVEL_OPTION)
def delete(
    host: 'localhost',
    port: 7707,
    loglevel: str
):
    set_logging_level(loglevel)


@cli.command(name="test", help="Unit test one or more job using data test definitions")
@cli_helpers.add_options(LOG_LEVEL_OPTION)
def test(
    loglevel: str
):
    set_logging_level(loglevel)


@cli.command(name="status", help="Display status and statistics of running and completed jobs")
@cli_helpers.add_options(LOG_LEVEL_OPTION)
def status(
    loglevel: str
):
    set_logging_level(loglevel)


@cli.command(name="stop", help="Stops a job")
@cli_helpers.add_options(LOG_LEVEL_OPTION)
def stop(
    loglevel: str
):
    set_logging_level(loglevel)


@cli.command(name="start", help="Starts a job")
@cli_helpers.add_options(LOG_LEVEL_OPTION)
def start(
    loglevel: str
):
    set_logging_level(loglevel)


@cli.command(name="preview", help="Preview a job using sample data")
@cli_helpers.add_options(LOG_LEVEL_OPTION)
def trace(
    loglevel: str
):
    set_logging_level(loglevel)


def set_logging_level(loglevel: str):
    """Sets logger's log level

    Args:
        loglevel(str): log level
    """
    logger.setLevel(loglevel.upper())
    ch.setLevel(loglevel.upper())


def main():
    cli()


if __name__ == '__main__':
    cli()
