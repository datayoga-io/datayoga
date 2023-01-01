from dataclasses import dataclass
from enum import Enum
from typing import Optional

Status = Enum("Status", "SUCCESS REJECTED FILTERED")


@dataclass
class Result():
    status: Status
    message: Optional[str] = None


SUCCESS = Result(Status.SUCCESS)
FILTERED = Result(Status.FILTERED)
