import json
import os
import re
import sys
from os import path
from typing import Any, Dict, List

import yaml
from datayoga_core.context import Context


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


def format_block_properties(properties: Dict[str, Any]) -> Dict[str, Any]:
    """Adds `fields` array with the passed properties in case it's missing

    Args:
        properties (Dict[str, Any]): properties

    Returns:
        Dict[str, Any]: formatted properties with `fields` array
    """
    return {"fields": [properties]} if not "fields" in properties else properties


def is_bundled() -> bool:
    return getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS")


def get_bundled_dir() -> str:
    datayoga_dir = path.join(sys._MEIPASS, "datayoga")
    return datayoga_dir if os.path.isdir(datayoga_dir) else sys._MEIPASS


def get_resource_path(relative_path: str) -> str:
    if is_bundled():
        # we are running in a bundle
        return path.join(get_bundled_dir(), "resources", relative_path)
    else:
        # we are running in a normal Python environment
        return path.join(os.path.dirname(__file__), "resources", relative_path)


def split_field(field: str) -> List[str]:
    return re.split(r"(?<!\\)\.", field)


def unescape_field(field: str) -> str:
    return field.replace("\\.", ".")


def get_connection_details(connection_name: str, context: Context) -> Dict[str, Any]:
    if context:
        connection = next(filter(lambda x: x["name"] == connection_name, context.properties.get("connections")), None)
        if connection:
            return connection

    raise ValueError(f"{connection_name} connection not found")
