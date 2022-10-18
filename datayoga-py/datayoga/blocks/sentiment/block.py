from concurrent import futures
import json
import requests
import logging
from typing import Any, Dict, List

from datayoga.block import Block as DyBlock
from datayoga.context import Context

logger = logging.getLogger("dy")


class Block(DyBlock):

    def run(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        logger.debug(f"Running {self.get_block_name()}")
        return_data = []

        def get_sentiment(row):
            return requests.post(
                'http://localhost:9000/?properties={"annotators":"sentiment","outputFormat":"json"}',
                data={'data': row[self.properties["field"]]}).text

        # you can increase the amount of workers, it would increase the amount of thread created
        with futures.ThreadPoolExecutor(max_workers=20) as executor:
            res = executor.map(get_sentiment, data)
            for index, response in enumerate(res):
                # add a list of the items with the new field
                sentiment = json.loads(response)["sentences"][0]["sentiment"]
                data[index][self.properties["target_field"]] = sentiment
                return_data.append(data[index])

        return return_data
