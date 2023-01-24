from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

Status = Enum("Status", "SUCCESS REJECTED FILTERED")


@dataclass
class Result():
    status: Status
    payload: Optional[Dict[str, Any]] = None
    message: Optional[str] = None


@dataclass
class BlockResult():
    processed: List[Result] = field(default_factory=list)
    filtered: List[Result] = field(default_factory=list)
    rejected: List[Result] = field(default_factory=list)

    # allow unpacking
    def __iter__(self):
        return iter((self.processed, self.filtered, self.rejected))


@dataclass
class JobResult(BlockResult):
    # FFU, separate type for JobResult
    pass


SUCCESS = Result(Status.SUCCESS)
FILTERED = Result(Status.FILTERED)
