from typing import Any, Dict, Optional


class Context():
    """
    Context

    Attributes:
        properties Dict[str, Any]: Context properties
        state (Dict[str, Any]): Context state
    """

    def __init__(self, properties: Optional[Dict[str, Any]] = None, state: Optional[Dict[str, Any]] = None):
        """
        Constructs a context

        Args:
            properties (Optional[Dict[str, Any]], optional): Context properties. Defaults to None.
            state (Optional[Dict[str, Any]], optional): Context state. Defaults to None.
        """
        self.properties = properties
        self.state = state
