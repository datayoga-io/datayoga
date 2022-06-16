import importlib
import logging
import os
from os import path
from typing import Any, Dict, List

from jsonschema import validate

from datayoga import utils
from datayoga.block import Block

logger = logging.getLogger(__name__)


class Job():

    def __init__(self, job_yaml: Dict[str, Any]):
        validate(instance=job_yaml, schema=utils.read_json(
            path.join(os.path.dirname(__file__), "schemas", "job.schema.json")))

        steps: List[Block] = []
        for step in job_yaml["steps"]:
            block_name = step["uses"]
            module_name = "blocks" + "." + block_name + "." + "block"
            module = importlib.import_module(module_name)

            block: Block = getattr(module, utils.snake_to_camel(block_name))(step["with"])
            steps.append(block)

        self.steps = steps

    def transform(self, data: List[Dict[str, Any]], context: Any = None) -> List[Dict[str, Any]]:
        for step in self.steps:
            data = step.transform(data, context)

        return data
