import importlib
import logging
import os
import sys
from enum import Enum
from os import path
from typing import Any, Dict, List, Optional, Tuple

from datayoga_core import utils
from datayoga_core.context import Context
from jsonschema import validate

logger = logging.getLogger("dy")


Result = Enum("Result", "SUCCESS REJECTED FILTERED")


class Block():
    MSG_ID_FIELD = "__$$msg_id"
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

    def init(self, context: Optional[Context] = None):
        """
        Initializes block (abstract, should be implemented by the sub class)

        Args:
            context (Context, optional): Context. Defaults to None.
        """
        pass

    async def run(self, data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Result]]:
        """ Transforms data (abstract, should be implemented by the sub class)

        Args:
            data (List[Dict[str, Any]]): Data

        Returns:
            List[Dict[str, Any]]: Transformed data
        """
        pass

    def get_block_name(self):
        return os.path.basename(os.path.dirname(sys.modules[self.__module__].__file__))


#
# static utility methods
#

def create_block(block_name: str, properties: Dict[str, Any]) -> Block:
    module_name = f"datayoga_core.blocks.{block_name}.block"
    module = importlib.import_module(module_name)
    block: Block = getattr(module, "Block")(properties)
    return block
