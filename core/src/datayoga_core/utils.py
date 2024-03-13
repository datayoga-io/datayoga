import copy
import os
import re
import sys
import uuid
from os import path
from typing import Any, Dict, List, Union

import orjson
import yaml
from datayoga_core import result
from datayoga_core.block import Block
from datayoga_core.expression import JMESPathExpression, Language
from datayoga_core.result import BlockResult, Result, Status


def read_json(filename: str) -> Any:
    """Loads a filename as a JSON object.

    Args:
        filename (str): JSON filename to load.

    Returns:
        Any: JSON object.
    """
    with open(filename, "r", encoding="utf8") as json_file:
        return orjson.loads(json_file.read())


def read_yaml(filename: str) -> Dict[str, Any]:
    """Loads a filename as a YAML object.

    Args:
        filename (str): YAML filename to load.

    Raises:
        ValueError: In case of invalid YAML.

    Returns:
         Dict[str, Any]: YAML python object.
    """
    try:
        with open(filename, "r", encoding="utf8") as yaml_file:
            return yaml.safe_load(yaml_file)
    except Exception as e:
        raise ValueError(f"Malformed YAML: {e}")


def format_block_properties(properties: Dict[str, Any]) -> Dict[str, Any]:
    """Adds `fields` array with the passed properties in case it's missing.

    Args:
        properties (Dict[str, Any]): properties.

    Returns:
        Dict[str, Any]: formatted properties with `fields` array.
    """
    return {"fields": [properties]} if not "fields" in properties else properties


def prepare_expression(language: Language, expr: Union[Dict, str]) -> str:
    """Prepares the expression to compile.

    The SQL expression will be dumped as JSON.
    For JMESPath, the expression will be generated.

    Args:
        language (Language): The language for which the expression is prepared (SQL or JMESPath).
        expr (Union[Dict, str]): The expression to prepare.

    Raises:
        ValueError: If the map expression is not in a JSON-like format enclosed in { }.

    Returns:
        str: The prepared expression.
    """
    def prepare_key(key: str) -> str:
        return f'"{key}"' if " " in key or "-" in key else key

    def prepare_value(value):
        if isinstance(value, dict):
            return prepare_expression(language, value)

        return value

    if language == Language.SQL:
        return orjson.dumps(expr).decode() if isinstance(expr, dict) else expr.strip()

    # If there is an object here, we generate an expression.
    if isinstance(expr, dict):
        expr = ", ".join(f'{prepare_key(k)}: {prepare_value(v)}' for k, v in expr.items())
        expr = f"{{{expr}}}"

    expression_prop = expr.strip()

    if not (expression_prop.startswith("{") and expression_prop.endswith("}")):
        raise ValueError("map expression must be in a json-like format enclosed in { }")

    return expression_prop


def is_bundled() -> bool:
    return getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS")


def get_bundled_dir() -> str:
    datayoga_dir = path.join(sys._MEIPASS, "datayoga_core")
    return datayoga_dir if os.path.isdir(datayoga_dir) else sys._MEIPASS


def get_resource_path(relative_path: str) -> str:
    if is_bundled():
        # we are running in a bundle
        return path.join(get_bundled_dir(), "resources", relative_path)
    else:
        # we are running in a normal Python environment
        return path.join(os.path.dirname(__file__), "resources", relative_path)


def set_field(obj: Dict[str, Any], field_name: str, value: Any):
    """Sets the value of a field in a nested dictionary.

    Args:
        obj (Dict): The dictionary to modify.
        field_name (str): Field name representing the path to the field, e.g., "nested.field".
        value (Any): The value to set.

    Example:
        set_field(obj, "nested.field", 42) will set obj["nested"]["field"] = 42.
    """
    field_path = split_field(field_name)
    for key in field_path[:-1]:
        key = unescape_field(key)
        obj = obj.setdefault(key, {})  # Setdefault creates missing nested keys as dictionaries

    obj[unescape_field(field_path[-1])] = value


def split_field(field: str, *, __expression=re.compile(r"(?<!\\)\.")) -> List[str]:
    r"""Splits a string by dots, similar to str.split(), but allows escaping a dot with a backslash.
    Example:
        a.b\.c.d => ["a", "b\\.c", "d"]
    """
    return __expression.split(field)


def unescape_field(field: str) -> str:
    r"""Replaces "\\." by "."."""
    return field.replace("\\.", ".")


def all_success(records: List[Dict[str, Any]]) -> BlockResult:
    return BlockResult(processed=[Result(Status.SUCCESS, payload=row) for row in records])


def is_rejected(record: Dict[str, Any]) -> bool:
    return record.get(Block.RESULT_FIELD, result.SUCCESS).status == Status.REJECTED


def add_uid(record: Dict[str, Any]) -> Dict[str, Any]:
    return {Block.MSG_ID_FIELD: f"{uuid.uuid4()}", **record}


def remove_msg_id(record: dict) -> dict:
    return {k: v for k, v in record.items() if k != Block.MSG_ID_FIELD}


def explode_records(records: List[Dict[str, Any]], field_expression: str) -> List[Dict[str, Any]]:
    """Takes a list of records and a JMESPath expression specifying a field to be exploded, and returns a new list of records
    where each record has been "exploded" into multiple records based on the values in the specified field.

    Args:
        records (List[Dict[str, Any]]): A list of dictionaries representing records.
        field_expression (str): A string specifying the field to be exploded, in the form "field_name: expression".

    Returns:
        List[Dict[str, Any]]: A new list of dictionaries representing the exploded records.
    """
    field_name, expression = map(str.strip, field_expression.split(":", maxsplit=1))

    jmespath_expr = JMESPathExpression()
    jmespath_expr.compile(expression)

    exploded_records = []

    for record in records:
        # Apply the JMESPath expression to the current record to obtain the values to be exploded.
        field_values = jmespath_expr.search(record)

        # If the JMESPath expression returned any values, create a new record for each value and add it to the output list.
        if field_values:
            for field_value in field_values:
                new_record = copy.deepcopy(record)
                new_record[field_name] = field_value
                exploded_records.append(new_record)

    return exploded_records
