from abc import abstractmethod
from typing import Any, AsyncGenerator, Dict, List

from .block import Block


class Message:
    def __init__(self, msg_id: str, value: Dict[str, Any]):
        self.msg_id = msg_id
        self.value = value


class Producer(Block):

    @abstractmethod
    async def produce(self) -> AsyncGenerator[List[Message], None]:
        """Produces data

        Returns:
            AsyncGenerator[List[Message], None]: A generator of message batches.
        """
        raise NotImplementedError

    def ack(self, msg_ids: List[str]):
        """Sends acknowledge for the message IDs of the records that have been processed

        Args:
            msg_ids (List[str]): Message IDs
        """
        pass
