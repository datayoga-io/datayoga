from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, unique
from typing import Any, Dict, List, Optional


@unique
class Status(str, Enum):
    SUCCESS = "SUCCESS"
    REJECTED = "REJECTED"
    FILTERED = "FILTERED"


@dataclass
class Result:
    status: Status
    payload: Optional[Dict[str, Any]] = None
    message: Optional[str] = None


@dataclass
class BlockResult:
    processed: List[Result] = field(default_factory=list)
    filtered: List[Result] = field(default_factory=list)
    rejected: List[Result] = field(default_factory=list)

    def extend(self, other: BlockResult):
        self.processed.extend(other.processed)
        self.filtered.extend(other.filtered)
        self.rejected.extend(other.rejected)

    # allow unpacking
    def __iter__(self):
        return iter((self.processed, self.filtered, self.rejected))


@dataclass
class JobResult(BlockResult):
    # FFU, separate type for JobResult
    pass


SUCCESS = Result(Status.SUCCESS)
FILTERED = Result(Status.FILTERED)
