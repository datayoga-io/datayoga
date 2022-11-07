#!/usr/bin/env python
import asyncio
import logging
import os
import shutil
from os import path
from pathlib import Path

import click
import datayoga_core as dy
import jsonschema
from datayoga import cli_helpers
from datayoga.cli_helpers import handle_critical
from datayoga_core import utils
from pkg_resources import get_distribution

CONTEXT_SETTINGS = dict(max_content_width=120)
LOG_LEVEL_OPTION = [click.option(
    "--loglevel", '-log-level', type=click.Choice(
        ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"],
        case_sensitive=False),
    default="INFO", show_default=True)]

logger = logging.getLogger("dy")

# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setFormatter(cli_helpers.CustomFormatter())
logger.addHandler(ch)


@click.group(name="datayoga", help="DataYoga command line tool")
@click.version_option(get_distribution("datayoga").version)
def cli():
    pass


@cli.command(name="init", help="Scaffolds a new folder with sample configuration files")
@click.argument("project_name")
@cli_helpers.add_options(LOG_LEVEL_OPTION)
def init(
    project_name: str,
    loglevel: str
):
    set_logging_level(loglevel)

    try:
        if os.path.exists(project_name):
            raise ValueError(f"{project_name} is already exists")

        logger.debug(f"Creating {project_name} directory")
        shutil.copytree(utils.get_resource_path("scaffold"), project_name)

        # Explicitly set permissions for created files and directories, because in some cases this may not be correct.
        for root, dirs, files in os.walk(project_name):
            for paths, permissions in ((files, "644"), (dirs, "755")):
                for path in paths:
                    os.chmod(os.path.join(root, path), int(permissions, 8))

        print(f"Initialized {project_name} successfully")
    except Exception as e:
        handle_critical(logger, f"Error while initializing {project_name}", e)


@cli.command(name="validate", help="Validates a job in dry run mode")
@cli_helpers.add_options(LOG_LEVEL_OPTION)
def validate(
        loglevel: str
):
    set_logging_level(loglevel)


@cli.command(name="run", help="Runs a job", context_settings=CONTEXT_SETTINGS)
@click.argument("job")
@click.option('--dir', help="DataYoga directory", default=".", show_default=True)
@cli_helpers.add_options(LOG_LEVEL_OPTION)
def run(
    job: str,
    dir: str,
    loglevel: str
):
    set_logging_level(loglevel)

    try:
        logger.info("Runner started...")
        job_file = path.join(dir, "jobs", job.replace(".", os.sep) + ".yaml")
        job_settings = utils.read_yaml(job_file)
        logger.debug(f"job_settings: {job_settings}")

        connections = utils.read_yaml(path.join(dir, "connections.yaml"))
        logger.debug(f"connections: {connections}")

        jsonschema.validate(instance=connections, schema=utils.read_json(
            utils.get_resource_path(os.path.join("schemas", "connections.schema.json"))))

        context = dy.Context({
            "connections": connections,
            "data_path": path.join(dir, "data"),
            "job_name": Path(job_file).stem
        })

        job = dy.compile(job_settings)

        producer = job.input
        logger.info(f"Producing from {producer.__module__}")
        job.init(context)
        asyncio.run(job.run())
    except Exception as e:
        cli_helpers.handle_critical(logger, "Error while running a job", e)


@cli.command(name="test", help="Unit test one or more job using data test definitions")
@cli_helpers.add_options(LOG_LEVEL_OPTION)
def test(
    loglevel: str
):
    set_logging_level(loglevel)


@cli.command(name="status", help="Displays status and statistics of running and completed jobs")
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


@cli.command(name="preview", help="Previews a job using sample data")
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


if __name__ == "__main__":
    cli()