import importlib
import logging
import os
import re
from os import path
from pathlib import Path
from typing import Any, Dict

from jsonschema import validate

from datayoga import utils
from datayoga.dy_block import Block

logger = logging.getLogger(__name__)


def snake_to_camel(value: str) -> str:
    return re.sub(r'(?:^|_)(\w)', lambda x: x.group(1).upper(), value)


def compile(job: Dict[str, Any]):
    validate(instance=job, schema=utils.read_json(path.join(os.path.dirname(__file__), "schemas", "job.schema.json")))

    for step in job["steps"]:
        block_name = step["uses"]
        module_name = "blocks" + "." + block_name + "." + "block"
        module = importlib.import_module(module_name)

        block: Block = getattr(module, snake_to_camel(block_name))(step["with"])
        block.validate()
        block.init()
        block.transform(None)
        logger.info(f"step: {step}")


def transform(job: Dict[str, Any]):
    compile(job)
    return "hello"
