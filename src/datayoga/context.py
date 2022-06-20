from typing import Any, Dict


class Context():
    """
    Context

    Attributes:
        properties Dict[str, Any]: Context properties
        state (Dict[str, Any]): Context state
    """

    def __init__(self, properties: Dict[str, Any], state: Dict[str, Any]):
        """
        Constructs a context

        Args:
            properties (Dict[str, Any]): Context properties
            state (Dict[str, Any]): Context state
        """
        self.properties = properties
        self.state = state
