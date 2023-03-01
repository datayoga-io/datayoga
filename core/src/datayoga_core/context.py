from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class Context:
    """
    Context

    Attributes:
        properties Dict[str, Any]: Context properties
        state (Dict[str, Any]): Context state
    """
    properties: Optional[Dict[str, Any]] = None
    state: Optional[Dict[str, Any]] = None
