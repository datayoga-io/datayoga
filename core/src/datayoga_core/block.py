from __future__ import annotations

import importlib
import logging
import os
import sys
from abc import ABCMeta, abstractmethod
from os import path
from typing import Any, Dict, List, Optional, Tuple

from datayoga_core import utils
from datayoga_core.context import Context
from datayoga_core.result import Result
from jsonschema import validate

logger = logging.getLogger("dy")


class Block(metaclass=ABCMeta):
    INTERNAL_FIELD_PREFIX = "__$$"
    MSG_ID_FIELD = f"{INTERNAL_FIELD_PREFIX}msg_id"
    RESULT_FIELD = f"{INTERNAL_FIELD_PREFIX}result"
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
        json_schema_file = path.join(
            utils.get_bundled_dir(), "blocks", self.get_block_name(),
            "block.schema.json") if utils.is_bundled() else path.join(
            os.path.dirname(os.path.realpath(sys.modules[self.__module__].__file__)),
            "block.schema.json")

        logger.debug(f"validating {self.properties} against {json_schema_file}")
        validate(instance=self.properties, schema=utils.read_json(json_schema_file))

    @abstractmethod
    def init(self, context: Optional[Context] = None):
        """
        Initializes block

        Args:
            context (Context, optional): Context. Defaults to None.
        """
        raise NotImplementedError

    @abstractmethod
    async def run(self, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Result]]:
        """ Transforms data

        Args:
            data (List[Dict[str, Any]]): Data

        Returns:
            List[Dict[str, Any]]: Transformed data
        """
        raise NotImplementedError

    def get_block_name(self):
        return os.path.basename(os.path.dirname(sys.modules[self.__module__].__file__))

    @staticmethod
    def create(block_name: str, properties: Dict[str, Any]) -> Block:
        module_name = f"datayoga_core.blocks.{block_name}.block"
        module = importlib.import_module(module_name)
        block: Block = getattr(module, "Block")(properties)
        return block

    @staticmethod
    def produce_data_and_results(data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Result]]:
        results: List[Result] = []
        for record in data:
            results.append(record.get(Block.RESULT_FIELD, Result.success()))
            if Block.RESULT_FIELD in record:
                del record[Block.RESULT_FIELD]

        logger.debug(f"data:{data}, results:{results}")
        return data, results

    @staticmethod
    def all_success(data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Result]]:
        return data, [Result.success()] * len(data)
