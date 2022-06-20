import logging
import os
import sys
from os import path
from typing import Any, Dict

from jsonschema import validate

from datayoga import utils
from datayoga.context import Context

logger = logging.getLogger(__name__)


class Block():
    """
    Block

    Attributes:
        properties Dict[str, Any]: Block properties
    """

    def __init__(self, properties: Dict[str, Any]):
        """
        Constructs a block

        Args:
            properties (Dict[str, Any]): Block [properties]
        """
        self.properties = properties
        self.validate()
        self.init()

    def validate(self):
        """
        Validates block against its JSON Schema
        """
        json_schema_file = path.join(os.path.dirname(os.path.realpath(
            sys.modules[self.__module__].__file__)), "block.schema.json")
        logger.debug(f"validating {self.properties} against {json_schema_file}")
        validate(instance=self.properties, schema=utils.read_json(json_schema_file))

    def init(self):
        """
        Initializes block (abstract, should be implemented by the sub class)
        """
        pass

    def transform(self, data: Any, context: Context = None) -> Any:
        """
        Transforms data

        Args:
            data (Any): Data
            context (Context, optional): Context. Defaults to None.

        Returns:
            Any: Transformed data
        """
        logger.debug(f"Transforming data, data before: {data}")

        transformed_data = self.run(data, context)
        logger.debug(f"Data after: {transformed_data}")

        return transformed_data

    def run(self, data: Any) -> Any:
        """ Transforms data (abstract, should be implemented by the sub class)

        Args:
            data (Any): Data

        Returns:
            Any: Transformed data
        """
        pass

    def get_block_name(self):
        return self.__class__.__name__
