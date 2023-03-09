from abc import ABCMeta, abstractmethod
from typing import Any, Dict, Generator, List, TypedDict

from .entity import Entity

Message = TypedDict("Message", {'msg_id': str, 'value': Dict[str, Any]})


class Producer(Entity, metaclass=ABCMeta):

    @abstractmethod
    def produce(self) -> Generator[Message, None, None]:
        """Produces data

        Returns:
            Generator[Message]: Produced data
        """
        raise NotImplementedError

    def ack(self, msg_ids: List[str]):
        """Sends acknowledge for the message IDs of the records that have been processed

        Args:
            msg_ids (List[str]): Message IDs
        """
        pass
