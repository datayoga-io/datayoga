import logging
import os
import sys
from os import path
from typing import Any, Dict

from jsonschema import validate

from datayoga import utils

logger = logging.getLogger(__name__)


class Block():

    def __init__(self, properties: Dict[str, Any]):
        self.properties = properties

    def validate(self):
        logger.info(f"*** validating: {self.properties} ***")
        logger.info(os.path.dirname(os.path.realpath(sys.modules[self.__module__].__file__)))
        validate(instance=self.properties, schema=utils.read_json(
            path.join(os.path.dirname(os.path.realpath(sys.modules[self.__module__].__file__)), "block.schema.json")))
