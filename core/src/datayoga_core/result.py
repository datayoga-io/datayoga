from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

Status = Enum("Status", "SUCCESS REJECTED FILTERED")


@dataclass
class Result():
    status: Status
    message: Optional[str] = None

    @staticmethod
    def success() -> Result:
        return Result(Status.SUCCESS)

    @staticmethod
    def reject(message: Optional[str] = None) -> Result:
        return Result(Status.REJECTED, message)
