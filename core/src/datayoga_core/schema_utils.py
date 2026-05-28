"""Schema composition helpers.

Producer block schemas use standard JSON Schema composition via `$ref` +
`allOf` (with `unevaluatedProperties: false` to allow inherited properties).
At validation time we want to keep the simple `jsonschema.validate(instance,
schema)` code path, so we resolve any local-file `$ref`s into the schema
ahead of time. The on-disk schemas remain standard JSON Schema; only the
in-memory form is flattened.

Example: a block schema like

    {"allOf": [{"$ref": "../../../resources/schemas/batchable.schema.json"}],
     "properties": {...},
     "unevaluatedProperties": false}

becomes

    {"allOf": [<contents of batchable.schema.json>],
     "properties": {...},
     "unevaluatedProperties": false}

after `resolve_refs(schema, schema_path)`.
"""
from __future__ import annotations

import copy
from os import path
from typing import Any, Dict, Optional, Set

from datayoga_core import utils


def resolve_refs(schema: Dict[str, Any], schema_path: Optional[str] = None) -> Dict[str, Any]:
    """Return a copy of `schema` with local-file `$ref`s inlined recursively.

    Args:
        schema: The schema to resolve.
        schema_path: Filesystem path the schema was loaded from. Used to
            resolve relative `$ref` paths. If None, refs are resolved against
            the bundled/non-bundled resources/schemas directory.

    Returns:
        A new schema with all local-file $refs replaced by the referenced
        document's contents. Non-local refs (http://, #fragments) and
        non-existent files pass through unchanged or raise depending on form.

    Raises:
        FileNotFoundError: A local-file $ref points at a file that doesn't exist.
        ValueError: A circular $ref chain is detected.
    """
    if schema_path is not None:
        base_dir = path.dirname(path.abspath(schema_path))
    else:
        base_dir = utils.get_resource_path("schemas")

    return _resolve_node(schema, base_dir, visited=set())


def _resolve_node(node: Any, base_dir: str, visited: Set[str]) -> Any:
    if isinstance(node, dict):
        ref = node.get("$ref")
        if isinstance(ref, str) and _is_local_file_ref(ref):
            target = path.normpath(path.join(base_dir, ref))
            if target in visited:
                raise ValueError(f"Circular $ref detected resolving '{ref}' at {target}")
            if not path.isfile(target):
                raise FileNotFoundError(
                    f"$ref target not found: '{ref}' resolved to {target}"
                )
            fragment = utils.read_json(target)
            visited.add(target)
            try:
                resolved = _resolve_node(fragment, path.dirname(target), visited)
            finally:
                visited.discard(target)
            return resolved
        return {k: _resolve_node(v, base_dir, visited) for k, v in node.items()}
    if isinstance(node, list):
        return [_resolve_node(item, base_dir, visited) for item in node]
    return copy.copy(node)


def _is_local_file_ref(ref: str) -> bool:
    """A $ref is a local file ref if it looks like a path to a .json/.schema.json
    file with no URI scheme and no in-document fragment."""
    if ref.startswith("#") or "://" in ref:
        return False
    return ref.endswith(".json")
