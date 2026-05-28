"""Schema composition helpers.

Producers and other blocks can declare `"$inherit": ["batchable"]` at the
top of their block.schema.json to pull in shared property definitions from
the fragments in resources/schemas/. `resolve_inherits` merges the
fragments' `properties` into the local schema (local properties win), then
removes the `$inherit` key. Schemas without `$inherit` are returned as-is.
"""
from __future__ import annotations

import copy
from os import path
from typing import Any, Dict, List

from datayoga_core import utils


def resolve_inherits(schema: Dict[str, Any], schemas_dir: str = None) -> Dict[str, Any]:
    """Merge any fragments listed in $inherit into the schema's properties.

    Args:
        schema: The schema to resolve. Mutated in place and also returned.
        schemas_dir: Directory containing the fragment files. Defaults to
            the bundled/non-bundled resources/schemas directory.

    Returns:
        The mutated schema with $inherit removed and fragment properties merged.
    """
    inherits: List[str] = schema.get("$inherit") or []
    if not inherits:
        return schema

    if schemas_dir is None:
        schemas_dir = utils.get_resource_path("schemas")

    merged_properties: Dict[str, Any] = {}
    for fragment_name in inherits:
        fragment_path = path.join(schemas_dir, f"{fragment_name}.schema.json")
        if not path.isfile(fragment_path):
            raise FileNotFoundError(
                f"Schema fragment '{fragment_name}' not found at {fragment_path}"
            )
        fragment = utils.read_json(fragment_path)
        merged_properties.update(copy.deepcopy(fragment.get("properties", {})))

    # Local properties take precedence over inherited ones.
    local_properties = schema.get("properties", {})
    merged_properties.update(local_properties)

    schema["properties"] = merged_properties
    schema.pop("$inherit", None)
    return schema
