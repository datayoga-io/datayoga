import logging
from abc import abstractmethod
from typing import Any, Dict, Generator, List, TypedDict

from .block import Block

logger = logging.getLogger("dy")
Message = TypedDict("Message", msg_id=str, value=Dict[str, Any])


class Producer(Block):

    @abstractmethod
    def produce(self) -> Generator[Message, None, None]:
        """ Produces data

        Returns:
            Generator[Message]: Produced data
        """
        raise NotImplementedError

    def ack(self, msg_ids: List[str]):
        """ Sends acknowledge for the message IDs of the records that have been processed

        Args:
            msg_ids (List[str]): Message IDs
        """
        pass
