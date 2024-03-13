#!/usr/bin/env python

import asyncio
import logging
import os
import shutil
import sys
from os import path
from pathlib import Path
from typing import Optional

import click
import datayoga_core as dy
import jsonschema
from datayoga_core import prometheus, utils
from datayoga_core.connection import Connection
from pkg_resources import DistributionNotFound, get_distribution

from datayoga import cli_helpers

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


def get_dy_distribution() -> str:
    try:
        return get_distribution("datayoga").version
    except DistributionNotFound:
        return "0.0.0"


@click.group(name="datayoga", help="DataYoga command line tool")
@click.version_option(get_dy_distribution())
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
        cli_helpers.handle_critical(logger, f"Error while initializing {project_name}", e)


@cli.command(name="validate", help="Validates a job in dry run mode")
@cli_helpers.add_options(LOG_LEVEL_OPTION)
def validate(
    loglevel: str
):
    set_logging_level(loglevel)


@cli.command(name="run", help="Runs a job", context_settings=CONTEXT_SETTINGS)
@click.argument("job_name")
@click.option('--dir', 'directory', help="DataYoga directory", default=".", show_default=True)
@click.option('--exporter-port', help="Enables Prometheus exporter on specified port", type=int)
@cli_helpers.add_options(LOG_LEVEL_OPTION)
def run(
    job_name: str,
    directory: str,
    exporter_port: Optional[int],
    loglevel: str
):
    set_logging_level(loglevel)

    logger.info("Runner started...")

    # validate the connections
    connections_file = path.join(directory, "connections.dy.yaml")
    try:
        connections = utils.read_yaml(connections_file)
        logger.debug(f"connections: {connections}")
        connections_schema = Connection.get_json_schema()
        logger.debug(f"connections_schema: {connections_schema}")
        jsonschema.validate(instance=connections, schema=connections_schema)
    except jsonschema.exceptions.ValidationError as schema_error:
        # print a validation message with the source lines
        cli_helpers.pprint_yaml_validation_error(connections_file, schema_error, logger)
        sys.exit(1)

    # validate the job
    job_file = path.join(directory, "jobs", job_name.replace(".", os.sep) + ".dy.yaml")
    try:
        job_settings = utils.read_yaml(job_file)
        logger.debug(f"job_settings: {job_settings}")

        context = dy.Context({
            "connections": connections,
            "data_path": path.join(directory, "data"),
            "job_name": Path(job_file).stem
        })

        job = dy.compile(job_settings)

        if exporter_port:
            prometheus.start(exporter_port)
            logger.info(f"Prometheus exporter started on port {exporter_port}")

        producer = job.producer
        logger.info(f"Producing from {producer.__module__}")
        job.init(context)
        asyncio.run(job.run())
    except jsonschema.exceptions.ValidationError as schema_error:
        # print a validation message with the source lines
        cli_helpers.pprint_yaml_validation_error(job_file, schema_error, logger)
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
