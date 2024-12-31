import logging
from abc import ABCMeta
from typing import Any, Dict, List, Optional

from datayoga_core import expression, utils
from datayoga_core.block import Block as DyBlock
from datayoga_core.context import Context
from datayoga_core.result import BlockResult, Result, Status

logger = logging.getLogger("dy")


class Block(DyBlock, metaclass=ABCMeta):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.properties = utils.format_block_properties(self.properties)

        self.fields = {}
        for prop in self.properties["fields"]:
            self.fields[prop["field"]] = expression.compile(prop["language"], prop["expression"])

    async def run(self, data: List[Dict[str, Any]]) -> BlockResult:
        logger.debug(f"Running {self.get_block_name()}")
        result = BlockResult()

        for field, expr in self.fields.items():
            try:
                # Try batch processing first
                expression_results = expr.search_bulk(data)

                # If successful, set fields for all records
                for i, row in enumerate(data):
                    utils.set_field(row, field, expression_results[i])
            except Exception as e:
                logger.debug(
                    f"Batch processing failed for field {field} with {e}, falling back to individual processing")

                # Process each record individually
                for row in data:
                    try:
                        single_result = expr.search(row)
                        utils.set_field(row, field, single_result)

                    except Exception as record_error:
                        # Add to rejected list with error message
                        result.rejected.append(Result(status=Status.REJECTED, payload=row, message=f"{record_error}"))
                        continue

                    # Add to processed list if successful
                    result.processed.append(Result(status=Status.SUCCESS, payload=row))

                return result

        # If we get here, batch processing was successful for all fields
        return utils.all_success(data)
