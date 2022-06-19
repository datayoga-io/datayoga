from typing import Any, Dict


class Context():

    def __init__(self, properties: Dict[str, Any], state: Dict[str, Any]):
        self.properties = properties
        self.state = state
