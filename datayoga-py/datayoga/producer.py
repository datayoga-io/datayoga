import logging
from typing import Any, Dict, Generator, List, TypedDict

from .block import Block

logger = logging.getLogger("dy")
Message = TypedDict("Message", msg_id=str, value=Dict[str, Any])


class Producer(Block):

    def produce(self) -> Generator[Message, None, None]:
        """ Produces data (abstract, should be implemented by the sub class)

        Returns:
            Generator[Message]: Produced data
        """
        pass

    def ack(self, msg_ids: List[str]):
        """ Sends acknowledge for the message IDs of the records that have been processed (abstract, should be implemented by the sub class)

        Args:
            msg_ids (List[str]): Message IDs
        """
        pass
