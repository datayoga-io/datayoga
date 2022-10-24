import asyncio
import copy
import importlib
import logging
import os
from typing import Any, Dict, List, Optional

import jsonschema

from datayoga import utils
from datayoga.block import Block
from datayoga.step import Step
from datayoga.context import Context

logger = logging.getLogger("dy")


class Job():
    """
    Job

    Attributes:
        steps List[Block]: List of steps
    """

    def __init__(self, steps: Optional[List[Dict[str, Any]]] = None, input: Optional[Dict[str, Any]] = None,
                 context: Optional[Context] = None, whitelisted_blocks: Optional[List[str]] = None):
        """
        Constructs a job and its blocks

        Args:
            job_steps (List[Dict[str, Any]]): Job steps
            context (Optional[Context], optional): Context. Defaults to None.
            input (Dict[str, Any]): Block to be used as a producer
            whitelisted_blocks: (Optional[List[str]], optional): Whitelisted blocks. Defaults to None.
        """
        self.whitelisted_blocks = whitelisted_blocks
        self.input = input
        self.context = context
        if steps is not None:
            self.set_blocks(steps)

    def set_blocks(self, block_definitions: List[Dict[str, Any]]):
        blocks: List[Block] = []
        for definition in block_definitions:
            block = self.get_block(definition)
            blocks.append(block)

        self.blocks = blocks

    def get_block(self, step: Dict[str, Any]) -> Block:
        block_name = step["uses"]
        if self.whitelisted_blocks and block_name not in self.whitelisted_blocks:
            raise ValueError(f"Using {block_name} block is prohibited")

        module_name = f"datayoga.blocks.{block_name}.block"
        module = importlib.import_module(module_name)
        block: Block = getattr(module, "Block")(step["with"], self.context)
        return block

    def load_json(self, json_source):
        jsonschema.validate(instance=json_source, schema=utils.read_json(
            utils.get_resource_path(os.path.join("schemas", "job.schema.json"))))
        self.set_blocks(json_source["steps"])
        if json_source.get("input") is not None:
            self.input = self.get_block(json_source.get("input"))

    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transforms data

        Args:
            data (List[Dict[str, Any]]): Data

        Returns:
            List[Dict[str, Any]]: Transformed data
        """
        transformed_data = copy.deepcopy(data)
        for step in self.blocks:
            transformed_data, results = asyncio.run(step.run(transformed_data))
            logger.debug(transformed_data)

        return transformed_data
