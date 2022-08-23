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

    def __init__(self, job_settings: Dict[str, Any]):
        """
        Constructs a job and its blocks

        Args:
            job_settings (Dict[str, Any]): Job settings
        """
        validate(instance=job_settings, schema=utils.read_json(
            path.join(os.path.dirname(__file__), "schemas", "job.schema.json")))

        steps: List[Block] = []
        for step in job_settings["steps"]:
            block_name = step["uses"]
            module_name = f"datayoga.blocks.{block_name}.block"
            try:
                module = importlib.import_module(module_name)
            except ModuleNotFoundError:
                raise ValueError(f"Block {block_name} does not exist")

            block: Block = getattr(module, "Block")(step["with"])
            steps.append(block)

        self.steps = steps

    def transform(self, data: List[Dict[str, Any]], context: Context = None) -> List[Dict[str, Any]]:
        """
        Transforms data

        Args:
            data (List[Dict[str, Any]]): Data
            context (Context, optional): Context. Defaults to None.

        Returns:
            List[Dict[str, Any]]: Transformed data
        """
        transformed_data = copy.deepcopy(data)
        for step in self.steps:
            transformed_data = step.transform(transformed_data, context)
        return transformed_data
