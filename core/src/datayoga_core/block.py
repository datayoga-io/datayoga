from __future__ import annotations

import importlib
import logging
import os
import sys
from abc import ABCMeta, abstractmethod
from os import path
from typing import Any, Dict, List, Optional

from datayoga_core import utils
from datayoga_core.context import Context
from datayoga_core.result import BlockResult
from jsonschema import validate

logger = logging.getLogger("dy")


class Block(metaclass=ABCMeta):
    INTERNAL_FIELD_PREFIX = "__$$"
    MSG_ID_FIELD = f"{INTERNAL_FIELD_PREFIX}msg_id"
    RESULT_FIELD = f"{INTERNAL_FIELD_PREFIX}result"
    OPCODE_FIELD = f"{INTERNAL_FIELD_PREFIX}opcode"
    """
    Block

    Attributes:
        properties Dict[str, Any]: Block properties
    """

    def __init__(self, properties: Dict[str, Any] = {}):
        """
        Constructs a block

        Args:
            properties (Dict[str, Any]): Block [properties]
        """
        self.properties = properties
        self.validate()

    def validate(self):
        """
        Validates block against its JSON Schema
        """
        logger.debug(f"validating {self.properties}")
        validate(instance=self.properties, schema=self.get_json_schema())

    def get_json_schema(self) -> Dict[str, Any]:
        """
        Returns the JSON Schema for this block

        Returns:
            Dict[str, Any]: JSON Schema
        """
        json_schema_file = path.join(
            utils.get_bundled_dir(),
            os.path.relpath(
                os.path.dirname(sys.modules[self.__module__].__file__),
                start=os.path.dirname(__file__)),
            "block.schema.json") if utils.is_bundled() else path.join(
            os.path.dirname(os.path.realpath(sys.modules[self.__module__].__file__)),
            "block.schema.json")
        logger.debug(f"loading schema from {json_schema_file}")
        return utils.read_json(json_schema_file)

    @abstractmethod
    def init(self, context: Optional[Context] = None):
        """
        Initializes block

        Args:
            context (Context, optional): Context. Defaults to None.
        """
        raise NotImplementedError

    async def run(self, data: List[Dict[str, Any]]) -> BlockResult:
        """ Transforms data

        Args:
            data (List[Dict[str, Any]]): Data

        Returns:
            BlockResult: Block result
        """
        pass

    def stop(self):
        """
        Cleans the block connections and state
        """
        pass

    def get_block_name(self):
        return os.path.basename(os.path.dirname(sys.modules[self.__module__].__file__))

    @staticmethod
    def create(block_name: str, properties: Dict[str, Any]) -> Block:
        module_name = f"datayoga_core.blocks.{block_name}.block"
        module = importlib.import_module(module_name)
        block: Block = getattr(module, "Block")(properties)
        return block
