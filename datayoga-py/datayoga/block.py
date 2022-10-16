import logging
import os
import sys
from os import path
from typing import Any, Dict, List, Optional

from jsonschema import validate

from datayoga import utils
from datayoga.context import Context

logger = logging.getLogger("dy")


class Block():
    """
    Block

    Attributes:
        properties Dict[str, Any]: Block properties
    """

    def __init__(self, properties: Dict[str, Any], context: Optional[Context] = None):
        """
        Constructs a block

        Args:
            properties (Dict[str, Any]): Block [properties]
            context (Optional[Context], optional): Context. Defaults to None.
        """
        self.properties = properties
        self.validate()
        self.init(context)

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

    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transforms data

        Args:
            data (List[Dict[str, Any]]): Data

        Returns:
            List[Dict[str, Any]]: Transformed data
        """
        logger.debug(f"Transforming data, data before: {data}")

        transformed_data = self.run(data)
        logger.debug(f"Data after: {transformed_data}")

        return transformed_data

    def run(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """ Transforms data (abstract, should be implemented by the sub class)

        Args:
            data (List[Dict[str, Any]]): Data

        Returns:
            List[Dict[str, Any]]: Transformed data
        """
        pass

    def ack(self, key: str):
        """ Sends acknowledge for a key of a record that has been processed

        Args:
            key (str): Record key 
        """
        pass

    def get_block_name(self):
        return os.path.basename(os.path.dirname(sys.modules[self.__module__].__file__))
