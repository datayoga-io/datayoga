#!/usr/bin/env python
import logging
import os
from os import path
from pathlib import Path
from typing import List

import click
import jsonschema
from datayoga import utils
from datayoga.context import Context
from datayoga.job import Job
from datayoga.utils import read_yaml
from pkg_resources import get_distribution
from tqdm import tqdm

from . import cli_helpers

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
@click.version_option(get_distribution("datayoga_cli").version)
def cli():
    pass


@cli.command(name="init", help="Scaffolds a new folder with sample configuration files")
@cli_helpers.add_options(LOG_LEVEL_OPTION)
def init(
    project_name: str,
    loglevel: str
):
    set_logging_level(loglevel)


@cli.command(name="validate", help="Validates a job in dry run mode")
@cli_helpers.add_options(LOG_LEVEL_OPTION)
def validate(
        loglevel: str
):
    set_logging_level(loglevel)


@cli.command(name="run", help="Runs a job", context_settings=CONTEXT_SETTINGS)
@click.argument("job-file")
@cli_helpers.add_options(LOG_LEVEL_OPTION)
def run(
    job_file: str,
    loglevel: str
):
    set_logging_level(loglevel)

    try:
        logger.info("Runner started...")

        job_settings = read_yaml(job_file)
        logger.debug(f"job_settings: {job_settings}")

        job_path = path.dirname(job_file)

        connections = read_yaml(path.join(job_path, "connections.yaml"))
        jsonschema.validate(instance=connections, schema=utils.read_json(
            utils.get_resource_path(os.path.join("schemas", "connections.schema.json"))))

        context = Context({
            "connections": connections,
            "data_path": path.join(job_path, "data"),
            "job_name": Path(job_file).stem
        })

        job = Job(job_settings, context)

        # assume the producer is the first block and remove it from job's steps
        producer = job.steps.pop(0)
        logger.info(f"Producing from {producer.__module__}")
        batch_size = producer.properties.get("batch_size", 2)
        batch = []
        keys = []

        def handle_batch(batch: List, keys: List):
            job.transform(batch)

            for key in keys:
                producer.ack(key)

        for record in tqdm(producer.run([])):
            key = record["key"]
            value = record["value"]
            logger.debug(f"Retrieved record:\n\tKey: {key}\n\tValue: {value}")
            keys.append(key)
            batch.append(value)

            if len(batch) == batch_size:
                handle_batch(batch, keys)

                batch = []
                keys = []

        # handle last batch
        if len(batch) > 0:
            handle_batch(batch, keys)

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


if __name__ == '__main__':
    cli()
