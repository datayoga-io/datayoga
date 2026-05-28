from unittest.mock import MagicMock

import pytest

from datayoga_core.blocks.relational.read.block import Block


async def _drain(producer):
    """Collects all batches emitted by a producer until end-of-stream."""
    out = []
    async for batch in producer.produce():
        out.append(batch)
    return out


def _fake_result(rows):
    """Builds a fake SQLAlchemy result that returns rows in fetchmany chunks."""
    state = {"i": 0}

    def fetchmany(n):
        i = state["i"]
        chunk = rows[i:i + n]
        state["i"] += len(chunk)
        return chunk

    res = MagicMock()
    res.fetchmany.side_effect = fetchmany
    res.execution_options.return_value = res
    return res


class _Row:
    """Stand-in for a SQLAlchemy Row exposing only `_asdict()`."""

    def __init__(self, d):
        """Stores the underlying dict that `_asdict()` will return."""
        self._d = d

    def _asdict(self):
        """Returns the stored dict, matching SQLAlchemy Row's API."""
        return self._d


def _mk_block(properties, fake_result):
    """Builds a relational/read Block without running its real init() (mocks engine/connection)."""
    block = Block.__new__(Block)
    block.properties = properties
    block.connection = MagicMock()
    block.tbl = MagicMock()
    block.tbl.select.return_value = "SELECT *"
    block.connection.execution_options.return_value.execute.return_value = fake_result
    return block


@pytest.mark.asyncio
async def test_relational_read_yields_batches_not_rows():
    rows = [_Row({"i": i}) for i in range(2500)]
    fake_result = _fake_result(rows)
    block = _mk_block({"batch_size": 1000}, fake_result)
    batches = await _drain(block)
    assert [len(b) for b in batches] == [1000, 1000, 500]


@pytest.mark.asyncio
async def test_relational_read_fetch_size_independent_of_batch_size():
    rows = [_Row({"i": i}) for i in range(5000)]
    fake_result = _fake_result(rows)
    block = _mk_block({"batch_size": 1000, "fetch_size": 2500}, fake_result)
    batches = await _drain(block)
    # Downstream batches are still batch_size=1000
    assert [len(b) for b in batches] == [1000, 1000, 1000, 1000, 1000]
    # Driver fetched in fetch_size=2500 chunks (2500 + 2500 + 0)
    fetch_sizes = [c.args[0] for c in fake_result.fetchmany.call_args_list]
    assert fetch_sizes[0] == 2500
    assert fetch_sizes[1] == 2500


@pytest.mark.asyncio
async def test_relational_read_default_fetch_size_is_10000():
    rows = [_Row({"i": i}) for i in range(500)]
    fake_result = _fake_result(rows)
    block = _mk_block({}, fake_result)
    await _drain(block)
    fetch_sizes = [c.args[0] for c in fake_result.fetchmany.call_args_list]
    assert fetch_sizes[0] == 10000
