import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from datayoga_core import utils
from datayoga_core.context import Context
from datayoga_core.job import Job
from datayoga_core.result import JobResult

logger = logging.getLogger("dy")


def compile(
        job_settings: Dict[str, Any],
        whitelisted_blocks: Optional[List[str]] = None) -> Job:
    """
    Compiles a job in YAML

    Args:
        job_settings (Dict[str, Any]): Job settings
        whitelisted_blocks: (Optional[List[str]], optional): Whitelisted blocks. Defaults to None.

    Returns:
        Job: Compiled job
    """
    logger.debug("Compiling job")
    return Job.compile(job_settings, whitelisted_blocks)


def validate(job_settings: Dict[str, Any], whitelisted_blocks: Optional[List[str]] = None):
    """
    Validates a job in YAML

    Args:
        job_settings (Dict[str, Any]): Job settings
        whitelisted_blocks: (Optional[List[str]], optional): Whitelisted blocks. Defaults to None.

    Raises:
        ValueError: When the job is invalid
    """
    logger.debug("Validating job")
    try:
        Job.validate(
            source=job_settings,
            whitelisted_blocks=whitelisted_blocks
        )
    except Exception as e:
        raise ValueError(e)


def transform(job_settings: Dict[str, Any],
              data: List[Dict[str, Any]],
              context: Optional[Context] = None,
              whitelisted_blocks: Optional[List[str]] = None) -> JobResult:
    """
    Transforms data against a certain job

    Args:
        job_settings (Dict[str, Any]): Job settings
        data (List[Dict[str, Any]]): Data to transform
        context (Optional[Context]): Context. Defaults to None.
        whitelisted_blocks: (Optional[List[str]]): Whitelisted blocks. Defaults to None.

    Returns:
        JobResult: Job result
    """
    job = compile(job_settings, whitelisted_blocks)
    job.init(context)
    logger.debug("Transforming data")
    return job.transform(data)


def get_connections_json_schema() -> Dict[str, Any]:
    # get the folder of all connection-specific schemas
    connection_schemas_folder = utils.get_resource_path(os.path.join("schemas", "connections"))
    # we traverse the json schemas for connection types
    schema_paths = Path(connection_schemas_folder).rglob("**/*.schema.json")
    connection_types = []
    connection_schemas = []
    for schema_path in schema_paths:
        connection_type = schema_path.name.split(".")[0]
        connection_types.append(connection_type)

        schema = utils.read_json(f"{schema_path}")
        # append to the array of allOf for the full schema
        connection_schemas.append({
            "if": {
                "properties": {
                    "type": {
                        "description": "Connection type",
                        "type": "string",
                        "const": connection_type
                    },
                },
                "required": ["type"]
            },
            "then": schema
        })

    connections_general_schema = utils.read_json(
        os.path.join(
            utils.get_bundled_dir() if utils.is_bundled() else os.path.dirname(os.path.realpath(__file__)),
            "resources", "schemas", "connections.schema.json"))
    connections_general_schema["patternProperties"]["."]["allOf"] = connection_schemas
    connections_general_schema["patternProperties"]["."]["properties"]["type"]["enum"] = connection_types

    return connections_general_schema
