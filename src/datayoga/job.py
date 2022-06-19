import copy
import importlib
import logging
import os
from os import path
from typing import Any, Dict, List

from jsonschema import validate

from datayoga import utils
from datayoga.block import Block
from datayoga.context import Context

logger = logging.getLogger(__name__)


class Job():
    """
    Job

    Attributes:
        steps List[Block]: List of steps
    """

    def __init__(self, job_yaml: Dict[str, Any]):
        """
        Constructs a job and its blocks

        Args:
            job_yaml (Dict[str, Any]): Job YAML
        """
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

    def transform(self, data: Any, context: Context = None) -> Any:
        """
        Transforms data

        Args:
            data (Any): Data
            context (Context, optional): Context. Defaults to None.

        Returns:
            Any: Transformed data
        """
        transformed_data = copy.deepcopy(data)
        for step in self.steps:
            transformed_data = step.transform(transformed_data, context)

        return transformed_data
