import logging
import os
from os import path
from pathlib import Path

import click
from datayoga import utils
from datayoga.context import Context
from datayoga.job import Job
from datayoga.utils import read_yaml
from jsonschema import validate

import cli_helpers
from cli_helpers import handle_critical

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


@click.group(name="datayoga", help="A command line tool to manage & configure DataYoga",
             context_settings=CONTEXT_SETTINGS)
def cli():
    pass


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

        connections = read_yaml(path.join(path.dirname(job_file), "connections.yaml"))
        validate(instance=connections, schema=utils.read_json(
            utils.get_resource_path(os.path.join("schemas", "connections.schema.json"))))

        context = Context({
            "connections": connections,
            "job_name": Path(job_file).stem
        })

        job = Job(job_settings, context)

        # assume the producer is the first block and remove it from job's steps
        producer = job.steps.pop(0)
        logger.info(f"Producer: {producer.__module__}")
        for record in producer.run([]):
            key = record["key"]
            value = record["value"]
            logger.info(f"Retrieved record:\n\tKey: {key}\n\tValue: {value}")
            job.transform([value])

            producer.ack(key)
    except Exception as e:
        handle_critical(logger, "Error while running a job", e)


def set_logging_level(loglevel: str):
    """Sets logger's log level

    Args:
        loglevel(str): log level
    """
    logger.setLevel(loglevel.upper())
    ch.setLevel(loglevel.upper())


if __name__ == "__main__":
    cli()
