from __future__ import annotations

import asyncio
import copy
import logging
import os
import sys
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import jsonschema
from datayoga_core import utils
from datayoga_core.block import Block, Result
from datayoga_core.context import Context
from datayoga_core.step import Step

logger = logging.getLogger("dy")


class ErrorHandling(Enum):
    ABORT = "abort"
    IGNORE = "ignore"


class Job():
    """
    Job

    Attributes:
        steps List[Block]: List of steps
    """

    def __init__(self, steps: Optional[List[Step]] = None, input: Optional[Block] = None,
                 error_handling: Optional[ErrorHandling] = None):
        """
        Constructs a job and its blocks

        Args:
            steps (List[Dict[str, Any]]): Job steps
            input (Optional[Block]): Block to be used as a producer
            error_handling (Optional[ErrorHandling]): error handling strategy
        """
        self.input = input
        self.steps = steps
        self.error_handling = error_handling if error_handling else ErrorHandling.IGNORE
        self.initialized = False

    def init(self, context: Optional[Context] = None):
        # open any connections or setup needed
        self.context = context
        self.root = None
        for step in self.steps:
            step.init(context)

            # create the step sequence
            if self.root is None:
                self.root = step
                last_step = self.root
            else:
                last_step = last_step.append(step)

        if self.input:
            self.input.init(context)

        self.root.add_done_callback(self.handle_result)
        self.initialized = True

    def transform(self, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Result]]:

        """
        Transforms data

        Args:
            data (List[Dict[str, Any]]): Data

        Returns:
            Tuple[List[Dict[str, Any]], List[Result]]: Transformed data and results
        """
        if not self.initialized:
            logger.debug("job has not been initialized yet, initializing...")
            self.init()

        transformed_data = copy.deepcopy(data)
        results = []
        for step in self.steps:
            transformed_data, results = asyncio.run(step.block.run(transformed_data))
            logger.debug(transformed_data)

        return transformed_data, results

    async def run(self):
        for record in self.input.produce():
            logger.debug(f"Retrieved record:\n\t{record}")
            await self.root.process([record])

        await self.shutdown()

    async def shutdown(self):
        # wait for in-flight records to finish
        await self.root.join()

        # graceful shutdown
        await self.root.stop()

    def handle_result(self, msg_ids: List[str], result: Result, reason: str):
        if result == Result.REJECTED and self.error_handling == ErrorHandling.ABORT.value:
            logger.critical("Aborting due to rejected record(s)")
            sys.exit(1)

        self.input.ack(msg_ids)

    @staticmethod
    def validate(source: Dict[str, Any], whitelisted_blocks: Optional[List[str]] = None):
        # validate against the schema
        jsonschema.validate(instance=source, schema=utils.read_json(
            utils.get_resource_path(os.path.join("schemas", "job.schema.json"))))

        # validate that steps do not use any non whitelisted blocks
        if whitelisted_blocks is not None:
            for step in source.get("steps"):
                if step.get("uses") not in whitelisted_blocks:
                    raise ValueError(f"use of invalid block type: {step.get('uses')}")

        # validate each block against its schema
        for step_definition in source.get("steps"):
            Block.create(step_definition.get("uses"), step_definition.get("with"))

    @staticmethod
    def compile(source: Dict[str, Any], whitelisted_blocks: Optional[List[str]] = None) -> Job:
        Job.validate(source, whitelisted_blocks=whitelisted_blocks)
        steps: List[Step] = []
        # parse the steps
        for step_definition in source.get("steps"):
            block_type = step_definition.get("uses")
            block: Block = Block.create(block_type, step_definition.get("with"))
            step: Step = Step(step_definition.get("id", block_type), block)
            steps.append(step)

        # parse the input
        input = None
        if source.get("input") is not None:
            input_definition = source.get("input")
            input = Block.create(input_definition.get("uses"), input_definition.get("with"))

        return Job(steps, input, source.get("error_handling"))
