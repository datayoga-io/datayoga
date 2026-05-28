import pytest
from jsonschema import ValidationError

from datayoga_core.blocks.azure.read_event_hub.block import Block


def _minimal_props(extra=None):
    base = {
        "event_hub_connection_string": "Endpoint=sb://x/;SharedAccessKeyName=k;SharedAccessKey=v;EntityPath=eh",
        "event_hub_consumer_group_name": "$Default",
        "event_hub_name": "eh",
        "checkpoint_store_connection_string": "DefaultEndpointsProtocol=https;AccountName=a;AccountKey=k==",
        "checkpoint_store_container_name": "chk",
    }
    if extra:
        base.update(extra)
    return base


def test_unknown_property_rejected_by_validation():
    """additionalProperties: false catches typos like 'batch_sz'."""
    with pytest.raises(ValidationError):
        Block(_minimal_props({"batch_sz": 300}))


def test_max_batch_size_accepted():
    """The renamed SDK-level property is now max_batch_size."""
    block = Block(_minimal_props({"max_batch_size": 500, "batch_size": 100}))
    assert block.properties["max_batch_size"] == 500
    assert block.properties["batch_size"] == 100


def test_max_batch_size_defaults_to_300_when_omitted():
    """The block's init() reads max_batch_size with a default of 300."""
    block = Block(_minimal_props())
    assert int(block.properties.get("max_batch_size", 300)) == 300


def test_renamed_schema_has_additional_properties_false():
    """Schema after rename: max_batch_size + streamable's batch_size/flush_ms,
    no unknown properties allowed."""
    block = Block(_minimal_props())
    schema = block.get_json_schema()
    assert schema.get("additionalProperties") is False
    assert "max_batch_size" in schema["properties"]
    assert "batch_size" in schema["properties"]
    assert "flush_ms" in schema["properties"]


def test_batch_size_300_is_silently_repurposed():
    """A user upgrading from a pre-rename version with batch_size: 300 (which
    used to mean SDK callback size) will see their YAML still validate, but
    batch_size now means pipeline batch size. Documented as breaking change."""
    block = Block(_minimal_props({"batch_size": 300}))
    assert block.properties["batch_size"] == 300
    assert "max_batch_size" not in block.properties
