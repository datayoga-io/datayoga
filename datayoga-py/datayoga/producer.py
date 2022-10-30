import logging
from typing import Any, Dict, List, TypedDict

from .block import Block

logger = logging.getLogger("dy")


class Producer(Block):

    def produce(self) -> List[TypedDict("Message", msg_id=str, value=Dict[str, Any])]:
        """ Produces data (abstract, should be implemented by the sub class)

        Returns:
            List[Dict[str, Any]]: Produced data
        """
        pass

    def ack(self, msg_id: str):
        """ Sends acknowledge for the message ID of a record that has been processed

        Args:
            msg_id (str): Message ID
        """
        pass
