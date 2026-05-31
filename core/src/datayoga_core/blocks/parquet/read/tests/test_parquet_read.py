from pathlib import Path

import pandas as pd
import pytest
from datayoga_core.blocks.parquet.read.block import Block


async def _drain(producer):
    """Collects all batches emitted by a producer until end-of-stream."""
    out = []
    async for batch in producer.produce():
        out.append(batch)
    return out


@pytest.fixture
def parquet_path(tmp_path) -> Path:
    """Writes a 2500-row Parquet file with three row groups (1000, 1000, 500)."""
    p = tmp_path / "data.parquet"
    df = pd.DataFrame({"i": list(range(2500))})
    from fastparquet import write as fp_write
    fp_write(str(p), df, row_group_offsets=1000)
    return p


@pytest.mark.asyncio
async def test_parquet_batches_to_batch_size(parquet_path):
    """2500 rows across three row groups, batch_size=1000 -> [1000, 1000, 500]."""
    block = Block({"file": str(parquet_path), "batch_size": 1000})
    block.init()
    batches = await _drain(block)
    assert [len(b) for b in batches] == [1000, 1000, 500]
    flat = [r for b in batches for r in b]
    assert flat[0]["i"] == 0
    assert all(Block.MSG_ID_FIELD in r for r in flat)


@pytest.mark.asyncio
async def test_parquet_rechunks_across_row_groups(parquet_path):
    """Batches honor batch_size regardless of underlying row-group boundaries."""
    # row groups are [1000, 1000, 500]; batch_size=750 should give batches of
    # [750, 750, 750, 250] regardless of row group boundaries.
    block = Block({"file": str(parquet_path), "batch_size": 750})
    block.init()
    batches = await _drain(block)
    assert [len(b) for b in batches] == [750, 750, 750, 250]
