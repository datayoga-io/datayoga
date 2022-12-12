from __future__ import annotations

from enum import Enum
from typing import Optional

Status = Enum("Status", "SUCCESS REJECTED FILTERED")


class Result():
    def __init__(self, status: Status, message: Optional[str] = None):
        self.status = status
        self.message = message

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__) and
            self.status == other.status and
            self.message == other.message)

    @staticmethod
    def success() -> Result:
        return Result(Status.SUCCESS)

    @staticmethod
    def reject(message: Optional[str] = None) -> Result:
        return Result(Status.REJECTED, message)
