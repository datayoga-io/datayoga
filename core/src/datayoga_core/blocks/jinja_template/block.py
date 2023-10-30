import logging
from abc import ABCMeta
from typing import Any, Dict, List, Optional

import jinja2
from datayoga_core import utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context
from datayoga_core.result import BlockResult, Result, Status

logger = logging.getLogger("dy")


class Block(DyBlock, metaclass=ABCMeta):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.field = self.properties["field"]
        self.template = jinja2.Template(self.properties["template"])

    async def run(self, data: List[Dict[str, Any]]) -> BlockResult:
        logger.debug(f"Running {self.get_block_name()}")

        block_result = BlockResult()
        field_path = utils.split_field(self.field)

        for _, row in enumerate(data):
            obj = row
            # handle nested fields. in that case, the obj points at the nested entity
            for key in field_path[:-1]:
                key = utils.unescape_field(key)
                obj = obj.setdefault(key, {})  # Setdefault creates missing nested keys as dictionaries

            try:
                # assign the new values
                obj[utils.unescape_field(field_path[-1:][0])] = self.template.render(**row)
                block_result.processed.append(Result(Status.SUCCESS, payload=row))
            except Exception as e:
                block_result.rejected.append(Result(status=Status.REJECTED, payload=row, message=f"{e}"))

        return block_result
