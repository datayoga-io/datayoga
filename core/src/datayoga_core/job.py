from __future__ import annotations

import asyncio
import logging
import marshal
import os
import sys
from contextlib import suppress
from enum import Enum, unique
from pathlib import Path
from typing import Any, Dict, List, Optional

import jsonschema
from datayoga_core import blocks, prometheus, utils
from datayoga_core.block import Block
from datayoga_core.context import Context
from datayoga_core.producer import Producer
from datayoga_core.result import JobResult, Result, Status
from datayoga_core.step import Step

logger = logging.getLogger("dy")


@unique
class ErrorHandling(str, Enum):
    ABORT = "abort"
    IGNORE = "ignore"


class Job:
    """Job.

    Attributes:
        steps List[Block]: List of steps.
    """

    def __init__(self, steps: Optional[List[Step]] = None, producer: Optional[Producer] = None,
                 error_handling: Optional[ErrorHandling] = None):
        """Constructs a job and its blocks.

        Args:
            steps (List[Dict[str, Any]]): Job steps.
            producer (Optional[Producer]): Block to be used as a producer.
            error_handling (Optional[ErrorHandling]): error handling strategy.
        """
        self.producer = producer
        self.steps = steps
        self.error_handling = error_handling if error_handling else ErrorHandling.IGNORE
        self.initialized = False
        self.root = None

    def init(self, context: Optional[Context] = None):
        # open any connections or setup needed
        self.context = context

        if self.steps is not None and len(self.steps) > 0:
            # set the first step as root
            self.root = self.steps[0]
            self.root.init(context)
            self.root.add_done_callback(self.handle_results)
            last_step = self.root

            # create the step sequence
            for step in self.steps[1:]:
                step.init(context)

                last_step = last_step.append(step)

        if self.producer:
            self.producer.init(context)

        self.initialized = True

    def transform(self, data: List[Dict[str, Any]], deepcopy: bool = True) -> JobResult:
        """Transforms data.

        Args:
            data (List[Dict[str, Any]]): Data.
            deepcopy (bool): If set to True, performs a deepcopy before modifying records;
                             otherwise, it modifies them in place. This can affect performance.

        Returns:
            JobResult: Job result.
        """
        if not self.initialized:
            logger.debug("job has not been initialized yet, initializing...")
            self.init()
        # use marshal. faster than deepcopy
        transformed_data = marshal.loads(marshal.dumps(data)) if deepcopy else data

        result = JobResult()
        # create an event loop for the duration of the transformation
        loop = asyncio.new_event_loop()

        try:
            for step in self.steps:
                try:
                    if len(transformed_data) == 0:
                        # in case all records have been filtered, stop sending
                        break
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

            # the processed records are those that make it to the end
            result.processed = [Result(Status.SUCCESS, payload=row) for row in transformed_data]
        finally:
            # close the event loop
            with suppress(NotImplementedError):
                # for pyodide. doesn't implement loop.close, ignore
                loop.close()

        return result

    async def async_transform(self, data: List[Dict[str, Any]], deepcopy: bool = True) -> JobResult:
        """Transforms data (async version).

        Args:
            data (List[Dict[str, Any]]): Data.
            deepcopy (bool): If set to True, performs a deepcopy before modifying records;
                             otherwise, it modifies them in place. This can affect performance.

        Returns:
            JobResult: Job result.
        """
        if not self.initialized:
            logger.debug("job has not been initialized yet, initializing...")
            self.init()
        # use marshal. faster than deepcopy
        transformed_data = marshal.loads(marshal.dumps(data)) if deepcopy else data

        result = JobResult()

        for step in self.steps:
            try:
                if len(transformed_data) == 0:
                    # in case all records have been filtered, stop sending
                    break
                processed, filtered, rejected = await step.block.run(transformed_data)
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

        # the processed records are those that make it to the end
        result.processed = [Result(Status.SUCCESS, payload=row) for row in transformed_data]

        return result

    async def run(self):
        async for records in self.producer.produce():
            prometheus.incoming_records.inc(len(records))

            logger.debug(f"Retrieved records:\n\t{records}")
            await self.root.process(records)

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

        self.producer.ack(msg_ids)

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
        input_block = None
        if source.get("input") is not None:
            input_definition = source.get("input")
            input_block = Block.create(input_definition.get("uses"), input_definition.get("with"))

        return Job(steps, input_block, source.get("error_handling"))

    @staticmethod
    def get_json_schema(whitelisted_blocks: Optional[List[str]] = None) -> Dict[str, Any]:
        """"Compiles a complete json schema of the job and all possible blocks"""
        block_schemas = []
        # we traverse the json schemas directly instead of 'walk_packages'
        # to avoid importing all of the block classes
        blocks_dir = (os.path.join(utils.get_bundled_dir(), "blocks") if utils.is_bundled() else
                      os.path.dirname(os.path.realpath(blocks.__file__)))
        block_types = []
        for schema_path in Path(blocks_dir).rglob("**/block.schema.json"):
            block_type = os.path.relpath(
                os.path.dirname(schema_path),
                os.path.dirname(os.path.realpath(blocks.__file__))
            )
            block_type = block_type.replace(os.path.sep, ".")

            if not (whitelisted_blocks is not None and block_type not in whitelisted_blocks):
                block_types.append(block_type)
                # load schema file
                schema = utils.read_json(f"{schema_path}")
                # append to the array of allOf for the full schema
                # we use allOf for better error reporting
                block_schemas.append({
                    "if": {
                        "properties": {
                            "uses": {
                                "description": "Block type",
                                "type": "string",
                                "const": block_type
                            },
                        },
                        "required": ["uses"]
                    },
                    "then": {
                        "properties": {
                            "with": schema,
                        }
                    }
                })

        job_schema = utils.read_json(
            os.path.join(
                utils.get_bundled_dir() if utils.is_bundled() else os.path.dirname(os.path.realpath(__file__)),
                "resources", "schemas", "job.schema.json"))
        job_schema["definitions"]["block"] = {
            "type": "object",
            "properties": {
                "uses": {
                    "enum": block_types,
                }
            },
            "allOf": block_schemas
        }

        return job_schema
