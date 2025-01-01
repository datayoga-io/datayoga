from abc import abstractmethod
from typing import Any, Dict, Generator, List

from datayoga_core.result import JobResult

from .block import Block


class Message:
    def __init__(self, msg_id: str, value: Dict[str, Any]):
        self.msg_id = msg_id
        self.value = value


class Producer(Block):

    @abstractmethod
    def produce(self) -> Generator[List[Message], JobResult, None]:
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
