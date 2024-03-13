import os
from pathlib import Path
from typing import Any, Dict

from datayoga_core import utils
from datayoga_core.context import Context


class Connection:
    @staticmethod
    def get_connection_details(connection_name: str, context: Context) -> Dict[str, Any]:
        if context and context.properties:
            connection = context.properties.get("connections", {}).get(connection_name)
            if connection:
                return connection

        raise ValueError(f"{connection_name} connection not found")

    @staticmethod
    def get_json_schema() -> Dict[str, Any]:
        """Compiles a complete JSON schema of the connection with all possible types"""
        connection_schemas = []

        connections_dir = os.path.join(
            utils.get_bundled_dir(),
            "connections") if utils.is_bundled() else os.path.dirname(
            os.path.realpath(__file__))

        for schema_path in Path(connections_dir).rglob("**/connections/*.schema.json"):
            connection_schemas.append(utils.read_json(f"{schema_path}"))

        connections_schema = utils.read_json(
            os.path.join(
                utils.get_bundled_dir() if utils.is_bundled() else os.path.dirname(os.path.realpath(__file__)),
                "resources", "schemas", "connections.schema.json"))

        connections_schema["definitions"]["connection"]["oneOf"] = connection_schemas

        return connections_schema
