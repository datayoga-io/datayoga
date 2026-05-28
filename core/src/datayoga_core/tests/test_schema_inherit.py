import json
from pathlib import Path

import pytest

from datayoga_core.schema_utils import resolve_inherits


SCHEMAS_DIR = (
    Path(__file__).resolve().parent.parent / "resources" / "schemas"
)


def test_inherit_merges_fragment_properties():
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
    schema = {"type": "object", "$inherit": ["streamable"], "properties": {}}
    resolved = resolve_inherits(schema, schemas_dir=str(SCHEMAS_DIR))
    assert "batch_size" in resolved["properties"]
    assert "flush_ms" in resolved["properties"]


def test_schema_without_inherit_unchanged():
    schema = {
        "type": "object",
        "properties": {"foo": {"type": "string"}},
        "additionalProperties": False,
    }
    resolved = resolve_inherits(schema, schemas_dir=str(SCHEMAS_DIR))
    assert resolved == schema


def test_unknown_fragment_raises():
    schema = {"type": "object", "$inherit": ["nope"], "properties": {}}
    with pytest.raises(FileNotFoundError):
        resolve_inherits(schema, schemas_dir=str(SCHEMAS_DIR))
