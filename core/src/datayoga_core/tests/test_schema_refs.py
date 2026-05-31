"""Tests for the $ref pre-resolver in `schema_utils.resolve_refs`.

Block schemas use standard JSON Schema composition (`allOf` + `$ref` to
local fragment files). We pre-resolve those refs at load time so the
in-memory schema is self-contained.
"""
import json
from pathlib import Path

import pytest
from datayoga_core.schema_utils import resolve_refs

SCHEMAS_DIR = Path(__file__).resolve().parent.parent / "resources" / "schemas"
BATCHABLE = SCHEMAS_DIR / "batchable.schema.json"


def test_resolve_refs_inlines_local_ref(tmp_path):
    """A {'$ref': 'localfile.json'} node is replaced inline with the file's contents."""
    fragment = {"type": "object", "properties": {"x": {"type": "integer"}}}
    frag_path = tmp_path / "frag.schema.json"
    frag_path.write_text(json.dumps(fragment))

    schema = {
        "type": "object",
        "allOf": [{"$ref": "frag.schema.json"}],
        "properties": {"y": {"type": "string"}},
    }
    schema_path = tmp_path / "host.schema.json"
    resolved = resolve_refs(schema, schema_path=str(schema_path))

    assert resolved["allOf"][0] == fragment
    assert "$ref" not in json.dumps(resolved)


def test_resolve_refs_no_ref_passthrough(tmp_path):
    """Schemas with no `$ref` come out structurally equal."""
    schema = {"type": "object", "properties": {"x": {"type": "string"}}}
    resolved = resolve_refs(schema, schema_path=str(tmp_path / "host.schema.json"))
    assert resolved == schema


def test_resolve_refs_resolves_transitively(tmp_path):
    """A fragment that itself contains `$ref` is resolved all the way."""
    leaf = {"type": "object", "properties": {"leaf_prop": {"type": "integer"}}}
    (tmp_path / "leaf.schema.json").write_text(json.dumps(leaf))

    middle = {"allOf": [{"$ref": "leaf.schema.json"}]}
    (tmp_path / "middle.schema.json").write_text(json.dumps(middle))

    schema = {"allOf": [{"$ref": "middle.schema.json"}]}
    resolved = resolve_refs(schema, schema_path=str(tmp_path / "host.schema.json"))

    # middle's $ref to leaf was resolved as part of the resolution of host's $ref to middle
    assert resolved == {"allOf": [{"allOf": [leaf]}]}


def test_resolve_refs_missing_file_raises(tmp_path):
    """A `$ref` pointing at a missing local file raises FileNotFoundError."""
    schema = {"allOf": [{"$ref": "does_not_exist.schema.json"}]}
    with pytest.raises(FileNotFoundError, match="does_not_exist.schema.json"):
        resolve_refs(schema, schema_path=str(tmp_path / "host.schema.json"))


def test_resolve_refs_detects_circular(tmp_path):
    """A → B → A cycle raises ValueError, not infinite recursion."""
    (tmp_path / "a.schema.json").write_text('{"allOf": [{"$ref": "b.schema.json"}]}')
    (tmp_path / "b.schema.json").write_text('{"allOf": [{"$ref": "a.schema.json"}]}')

    schema = {"allOf": [{"$ref": "a.schema.json"}]}
    with pytest.raises(ValueError, match="Circular"):
        resolve_refs(schema, schema_path=str(tmp_path / "host.schema.json"))


def test_resolve_refs_ignores_non_local_refs(tmp_path):
    """`$ref` values like '#/$defs/x' or 'http://...' are left untouched."""
    schema = {
        "allOf": [
            {"$ref": "#/$defs/internal"},
            {"$ref": "https://json-schema.org/draft/2019-09/schema"},
        ],
        "$defs": {"internal": {"type": "integer"}},
    }
    resolved = resolve_refs(schema, schema_path=str(tmp_path / "host.schema.json"))
    assert resolved == schema


def test_resolve_refs_against_real_fragment():
    """resolve_refs against the actual batchable fragment in the repo works."""
    # Simulate loading a block schema whose path is at depth blocks/X/Y/.
    schema = {
        "$schema": "https://json-schema.org/draft/2019-09/schema",
        "type": "object",
        "allOf": [{"$ref": "../../../resources/schemas/batchable.schema.json"}],
        "properties": {"connection": {"type": "string"}},
        "unevaluatedProperties": False,
    }
    # Pick any real block path so the relative $ref resolves.
    block_path = (
        Path(__file__).resolve().parent.parent
        / "blocks" / "std" / "read" / "block.schema.json"
    )
    resolved = resolve_refs(schema, schema_path=str(block_path))
    # The batchable fragment is inlined inside allOf
    assert resolved["allOf"][0]["properties"]["batch_size"]["default"] == 1000


def test_resolve_refs_default_base_dir():
    """When schema_path is None, refs resolve against resources/schemas/."""
    schema = {"allOf": [{"$ref": "batchable.schema.json"}]}
    resolved = resolve_refs(schema)
    assert resolved["allOf"][0]["properties"]["batch_size"]["default"] == 1000


def test_resolve_refs_default_base_dir_with_missing_file():
    """Without schema_path, refs pointing at unknown files in the resources dir raise."""
    schema = {"allOf": [{"$ref": "nope.schema.json"}]}
    with pytest.raises(FileNotFoundError):
        resolve_refs(schema)
