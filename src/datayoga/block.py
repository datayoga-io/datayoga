import logging
import os
import sys
from os import path
from typing import Any, Dict, List

from jsonschema import validate

from datayoga import utils

logger = logging.getLogger(__name__)


class Block():

    def __init__(self, properties: Dict[str, Any]):
        self.properties = properties
        self.validate()
        self.init()

    def validate(self):
        logger.info(f"*** validating: {self.properties} ***")
        logger.info(os.path.dirname(os.path.realpath(sys.modules[self.__module__].__file__)))
        validate(instance=self.properties, schema=utils.read_json(
            path.join(os.path.dirname(os.path.realpath(sys.modules[self.__module__].__file__)), "block.schema.json")))

    def init(self):
        pass

    def transform(self, data: List[Dict[str, Any]], context: Any = None) -> List[Dict[str, Any]]:
        logger.info("transform")

        return self.run(data, context)

    def run(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        pass
