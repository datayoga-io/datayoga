import asyncio
import copy
import importlib
import logging
import os
from typing import Any, Dict, List, Optional

import jsonschema

from datayoga import utils
from datayoga.block import Block, create_block
from datayoga.step import Step
from datayoga.context import Context

logger = logging.getLogger("dy")


class Job():
    """
    Job

    Attributes:
        steps List[Block]: List of steps
    """

    def __init__(self, steps: Optional[List[Step]] = None, input: Optional[Block] = None
                 ):
        """
        Constructs a job and its blocks

        Args:
            job_steps (List[Dict[str, Any]]): Job steps
            context (Optional[Context], optional): Context. Defaults to None.
            input (Dict[str, Any]): Block to be used as a producer
            whitelisted_blocks: (Optional[List[str]], optional): Whitelisted blocks. Defaults to None.
        """
        self.input = input
        self.steps = steps
        self.initialized = False

    def init(self, context: Optional[Context] = None):
        # open any connections or setup needed
        self.context = context
        self.initialized = True

    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transforms data

        Args:
            data (List[Dict[str, Any]]): Data

        Returns:
            List[Dict[str, Any]]: Transformed data
        """
        transformed_data = copy.deepcopy(data)
        loop = asyncio.get_event_loop()
        for step in self.steps:
            transformed_data, results = loop.run_until_complete(step.block.run(transformed_data))
            logger.debug(transformed_data)

        return transformed_data

#
# static utility methods
#


def validate_job(source: Dict[str, Any], whitelisted_blocks: Optional[List[str]] = None):
    # validate against the schema
    jsonschema.validate(instance=source, schema=utils.read_json(
        utils.get_resource_path(os.path.join("schemas", "job.schema.json"))))

    # validate that steps do not use any non whitelisted blocks
    if whitelisted_blocks is not None:
        for step in source.get("steps"):
            if step.get("uses") not in whitelisted_blocks:
                raise ValueError(f"use of invalid block type: {step.get('uses')}")


def compile_job(source: Dict[str, Any], whitelisted_blocks: Optional[List[str]] = None):
    validate_job(source, whitelisted_blocks=whitelisted_blocks)
    steps: List[Step] = []
    # parse the steps
    for step_definition in source.get("steps"):
        block_type = step_definition.get("uses")
        block_properties = step_definition.get("with")
        block: Block = create_block(block_type, block_properties)
        step: Step = Step(step_definition.get("id", block_type), block)
        steps.append(step)

    # parse the input
    input = None
    if source.get("input") is not None:
        input_definition = source.get("input")
        input = create_block(input_definition.get("uses"), input_definition.get("with"))

    return Job(steps, input)
