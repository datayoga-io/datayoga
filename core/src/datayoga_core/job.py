from __future__ import annotations

import asyncio
import logging
import marshal
import os
import sys
from enum import Enum, unique
from pathlib import Path
from typing import Any, Dict, List, Optional
from xmlrpc.client import boolean

import jsonschema
from datayoga_core import blocks, utils
from datayoga_core.block import Block
from datayoga_core.context import Context
from datayoga_core.result import JobResult, Result, Status
from datayoga_core.step import Step

logger = logging.getLogger("dy")

@unique
class ErrorHandling(str, Enum):
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
        self.root = None

    def init(self, context: Optional[Context] = None):
        # open any connections or setup needed
        self.context = context

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

        self.root.add_done_callback(self.handle_results)
        self.initialized = True

    def transform(self, data: List[Dict[str, Any]],
                  deepcopy: boolean = True) -> JobResult:
        """
        Transforms data

        Args:
            data (List[Dict[str, Any]]): Data
            deepcopy: if True, performs a deepcopy before modifying records. otherwise, modifies in place. can affect performance.

        Returns:
            JobResult: Job result
        """
        if not self.initialized:
            logger.debug("job has not been initialized yet, initializing...")
            self.init()
        # use marshal. faster than deepcopy
        transformed_data = marshal.loads(marshal.dumps(data)) if deepcopy else data

        result = JobResult()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # depending on context, you might debug or warning log that a running event loop wasn't found
            loop = asyncio.get_event_loop()

        for step in self.steps:
            try:
                processed, filtered, rejected = loop.run_until_complete(step.block.run(transformed_data))
                result.filtered.extend(filtered)
                result.rejected.extend(rejected)
                transformed_data = [result.payload for result in processed]
            except ConnectionError as e:
                # connection errors are thrown back to the caller to handle
                raise e
            except Exception as e:
                # other exceptions are rejected
                logger.error(f"Error while transforming data: {e}")
                result.rejected.extend(
                    [Result(Status.REJECTED, payload=row, message=f"{e}") for row in transformed_data])
                return result

        result.processed.extend([Result(Status.SUCCESS, payload=row) for row in transformed_data])
        return result

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

    def handle_results(self, msg_ids: List[str], results: List[Result]):
        if any(x.status == Status.REJECTED for x in results) and self.error_handling == ErrorHandling.ABORT:
            logger.critical("Aborting due to rejected record(s)")
            sys.exit(1)

        self.input.ack(msg_ids)

    @staticmethod
    def validate(source: Dict[str, Any], whitelisted_blocks: Optional[List[str]] = None):
        jsonschema.validate(instance=source, schema=Job.get_json_schema(whitelisted_blocks))

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

    @staticmethod
    def get_json_schema(whitelisted_blocks: Optional[List[str]] = None) -> Dict[str, Any]:
        # compiles a complete json schema of the job and all possible blocks
        block_schemas = []
        # we traverse the json schemas directly instead of 'walk_packages'
        # to avoid importing all of the block classes
        schema_paths = Path(os.path.join(utils.get_bundled_dir(), "blocks") if utils.is_bundled() else os.path.dirname(
            os.path.realpath(blocks.__file__))).rglob("**/block.schema.json")

        for schema_path in schema_paths:
            block_type = os.path.relpath(
                os.path.dirname(schema_path),
                os.path.dirname(os.path.realpath(blocks.__file__))
            )
            block_type = block_type.replace(os.path.sep, ".")

            if not (whitelisted_blocks is not None and block_type not in whitelisted_blocks):
                # load schema file
                schema = utils.read_json(f"{schema_path}")
                # append to the array of oneOf for the full schema
                block_schemas.append({
                    "type": "object",
                    "properties": {
                        "uses": {
                            "description": "Block type",
                            "type": "string",
                            "const": block_type
                        },
                        "with": schema,
                    },
                    "additionalProperties": False,
                    "required": ["uses"],
                })

        job_schema = utils.read_json(
            os.path.join(
                utils.get_bundled_dir() if utils.is_bundled() else os.path.dirname(os.path.realpath(__file__)),
                "resources", "schemas", "job.schema.json"))
        job_schema["definitions"]["block"] = {
            "type": "object",
            "oneOf": block_schemas
        }

        return job_schema
