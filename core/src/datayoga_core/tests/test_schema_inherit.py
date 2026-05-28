from pathlib import Path

import pytest
from datayoga_core.schema_utils import resolve_inherits

SCHEMAS_DIR = (
    Path(__file__).resolve().parent.parent / "resources" / "schemas"
)


def test_inherit_merges_fragment_properties():
    """A schema with $inherit:[batchable] picks up batch_size from the fragment."""
    schema = {
        "title": "demo",
        "type": "object",
        "$inherit": ["batchable"],
        "properties": {"foo": {"type": "string"}},
        "additionalProperties": False,
    }
    resolved = resolve_inherits(schema, schemas_dir=str(SCHEMAS_DIR))
    assert "$inherit" not in resolved
    assert "batch_size" in resolved["properties"]
    assert resolved["properties"]["batch_size"]["default"] == 1000
    assert resolved["properties"]["foo"] == {"type": "string"}
    assert resolved["additionalProperties"] is False


def test_inherit_local_property_wins_over_fragment():
    """When local schema redefines an inherited property, the local version takes precedence."""
    schema = {
        "type": "object",
        "$inherit": ["batchable"],
        "properties": {
            "batch_size": {"type": "integer", "minimum": 1, "default": 50}
        },
    }
    resolved = resolve_inherits(schema, schemas_dir=str(SCHEMAS_DIR))
    assert resolved["properties"]["batch_size"]["default"] == 50


def test_inherit_streamable_brings_both_props():
    """$inherit:[streamable] exposes both batch_size and flush_ms on the schema."""
    schema = {"type": "object", "$inherit": ["streamable"], "properties": {}}
    resolved = resolve_inherits(schema, schemas_dir=str(SCHEMAS_DIR))
    assert "batch_size" in resolved["properties"]
    assert "flush_ms" in resolved["properties"]


def test_schema_without_inherit_unchanged():
    """Schemas without $inherit pass through resolve_inherits unmodified."""
    schema = {
        "type": "object",
        "properties": {"foo": {"type": "string"}},
        "additionalProperties": False,
    }
    resolved = resolve_inherits(schema, schemas_dir=str(SCHEMAS_DIR))
    assert resolved == schema


def test_unknown_fragment_raises():
    """$inherit referencing a missing fragment file raises FileNotFoundError."""
    schema = {"type": "object", "$inherit": ["nope"], "properties": {}}
    with pytest.raises(FileNotFoundError):
        resolve_inherits(schema, schemas_dir=str(SCHEMAS_DIR))


def test_inherit_string_value_raises_type_error():
    """$inherit must be a list; passing a string raises TypeError loudly."""
    schema = {"type": "object", "$inherit": "batchable", "properties": {}}
    with pytest.raises(TypeError):
        resolve_inherits(schema, schemas_dir=str(SCHEMAS_DIR))


def test_inherit_non_string_items_raises_type_error():
    """Non-string items in the $inherit list raise TypeError."""
    schema = {"type": "object", "$inherit": ["batchable", 123], "properties": {}}
    with pytest.raises(TypeError):
        resolve_inherits(schema, schemas_dir=str(SCHEMAS_DIR))


def test_inherit_empty_list_returns_unchanged():
    """An empty $inherit list is a no-op; the schema is returned as-is."""
    schema = {"type": "object", "$inherit": [], "properties": {"foo": {}}}
    resolved = resolve_inherits(schema, schemas_dir=str(SCHEMAS_DIR))
    # Early-return path: schema is returned as-is (no mutation, no key removal).
    assert resolved is schema


def test_nested_inherit_raises_value_error(tmp_path):
    """A fragment that itself contains $inherit raises ValueError (no nested inheritance)."""
    # Build a fragment dir with a fragment that has its own $inherit.
    (tmp_path / "parent.schema.json").write_text(
        '{"properties": {"x": {"type": "string"}}}'
    )
    (tmp_path / "child.schema.json").write_text(
        '{"$inherit": ["parent"], "properties": {"y": {"type": "string"}}}'
    )
    schema = {"$inherit": ["child"], "type": "object", "properties": {}}
    with pytest.raises(ValueError, match="nested inheritance is not supported"):
        resolve_inherits(schema, schemas_dir=str(tmp_path))
