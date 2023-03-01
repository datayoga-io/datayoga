from abc import abstractmethod
from typing import Generator, List

from .block import Block, Message


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
