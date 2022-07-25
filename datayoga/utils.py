import json
from typing import Any, Dict

import yaml


def read_json(filename: str) -> Any:
    """
    Loads a filename as a JSON object

    Args:
        filename (str): JSON filename to load

    Returns:
        Any: JSON object
    """
    with open(filename, "r", encoding="utf8") as f:
        return json.load(f)


def read_yaml(filename: str) -> Dict[str, Any]:
    """
    Loads a filename as a YAML object

    Args:
        filename (str): YAML filename to load

    Raises:
        ValueError: In case of invalid YAML

    Returns:
         Dict[str, Any]: YAML python object
    """
    try:
        with open(filename, "r", encoding="utf8") as stream:
            return yaml.safe_load(stream)
    except Exception as e:
        raise ValueError(f"Malformed YAML: {e}")
