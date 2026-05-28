from pathlib import Path

import pytest
from datayoga_core.blocks.files.read_csv.block import Block


async def _drain(producer):
    """Collects all batches emitted by a producer until end-of-stream."""
    out = []
    async for batch in producer.produce():
        out.append(batch)
    return out


@pytest.fixture
def csv_path(tmp_path) -> Path:
    """Writes a 2500-row CSV with a single header row to a temp path."""
    p = tmp_path / "data.csv"
    rows = ["fname,lname"] + [f"first{i},last{i}" for i in range(2500)]
    p.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return p


@pytest.mark.asyncio
async def test_csv_batches_to_batch_size(csv_path):
    block = Block({"file": str(csv_path), "batch_size": 1000})
    block.init()
    batches = await _drain(block)
    assert [len(b) for b in batches] == [1000, 1000, 500]
    assert all(Block.MSG_ID_FIELD in r for b in batches for r in b)
    assert batches[0][0]["fname"] == "first0"


@pytest.mark.asyncio
async def test_csv_default_batch_size(csv_path):
    block = Block({"file": str(csv_path)})
    block.init()
    batches = await _drain(block)
    assert [len(b) for b in batches] == [1000, 1000, 500]
