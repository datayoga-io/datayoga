import statistics
from concurrent import futures
import json
import requests
import logging
from typing import Any, Dict, List

from datayoga.block import Block as DyBlock
from datayoga.context import Context

logger = logging.getLogger("dy")
SENTIMENTS = {
    "Verynegative": 0,
    "Negative": 1,
    "Neutral": 2,
    "Positive": 3,
    "Verypositive": 4
}
REVERSE_SENTIMENT = {v: k for k, v in SENTIMENTS.items()}


class Block(DyBlock):

    def run(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        logger.debug(f"Running {self.get_block_name()}")
        return_data = []

        def get_sentiment(row):
            return requests.post(
                'http://localhost:9000/?properties={"annotators":"sentiment","outputFormat":"json","timeout":30000}',
                data=row[self.properties["field"]].encode('utf-8')).text

        # you can increase the amount of workers, it would increase the amount of thread created
        with futures.ThreadPoolExecutor(max_workers=self.properties.get("processors", 4)) as executor:
            res = executor.map(get_sentiment, data)
            for index, response in enumerate(res):
                try:
                    # add a list of the items with the new field
                    logger.debug(response)
                    response_json = json.loads(response)["sentences"]
                    # calculate average sentiment (coreNLP is only by sentence)
                    sentiment = statistics.mean([SENTIMENTS[sentence["sentiment"]] for sentence in response_json])
                    sentiment = REVERSE_SENTIMENT[round(sentiment)]

                    data[index][self.properties["target_field"]] = sentiment
                except Exception:
                    pass
                return_data.append(data[index])

        return return_data
