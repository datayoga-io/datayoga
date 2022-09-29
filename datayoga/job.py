import copy
import importlib
import logging
import os
from typing import Any, Dict, List, Optional

from jsonschema import validate

from datayoga import utils
from datayoga.block import Block
from datayoga.context import Context

logger = logging.getLogger("dy")


class Job():
    """
    Job

    Attributes:
        steps List[Block]: List of steps
    """

    def __init__(self, job_steps: List[Dict[str, Any]], context: Optional[Context] = None):
        """
        Constructs a job and its blocks

        Args:
            job_steps (List[Dict[str, Any]]): Job steps
            context (Optional[Context], optional): Context. Defaults to None.
        """
        validate(instance=job_steps, schema=utils.read_json(
            utils.get_resource_path(os.path.join("schemas", "job.schema.json"))))

        steps: List[Block] = []
        for step in job_steps:
            block_name = step["uses"]
            module_name = f"datayoga.blocks.{block_name}.block"
            try:
                module = importlib.import_module(module_name)
            except ModuleNotFoundError:
                raise ValueError(f"Block {block_name} does not exist")

            block: Block = getattr(module, "Block")(step["with"], context)
            steps.append(block)

        self.steps = steps

    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transforms data

        Args:
            data (List[Dict[str, Any]]): Data

        Returns:
            List[Dict[str, Any]]: Transformed data
        """
        transformed_data = copy.deepcopy(data)
        for step in self.steps:
            transformed_data = step.transform(transformed_data)
        return transformed_data
