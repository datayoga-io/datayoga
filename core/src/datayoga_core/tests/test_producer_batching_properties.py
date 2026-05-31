"""Property-based tests for the Producer base-class rechunker.

Where `test_producer_batching.py` asserts specific outputs for specific inputs,
this file uses Hypothesis to generate arbitrary chunk-size sequences and probe
the rechunker's invariants. Catches the class of bug where the code works for
the inputs you tested but breaks somewhere in the wider input space.
"""
import asyncio
from typing import AsyncGenerator, Dict, List, Optional

import pytest
from datayoga_core.context import Context
from datayoga_core.producer import Producer
from hypothesis import given, settings
from hypothesis import strategies as st


class _ScriptedProducer(Producer):
    """Producer driven by a scripted list of chunk-sizes; each chunk has
    sequential integer payloads."""

    def __init__(self, properties, *, chunk_sizes):
        """Wires the schema and chunk script."""
        self._test_schema = {
            "type": "object",
            "properties": {"batch_size": {"type": "integer", "minimum": 1}},
        }
        self._chunk_sizes = chunk_sizes
        super().__init__(properties)

    def get_json_schema(self):
        """In-memory schema (no disk read)."""
        return self._test_schema

    def init(self, context: Optional[Context] = None):
        """No-op."""
        pass

    async def produce_chunks(self) -> AsyncGenerator[List[Dict], None]:
        """Yield chunks of the scripted sizes, with sequential payload ids."""
        counter = 0
        for size in self._chunk_sizes:
            chunk = [{Producer.MSG_ID_FIELD: str(counter + i), "v": counter + i}
                     for i in range(size)]
            counter += size
            yield chunk


async def _drain(producer: Producer):
    out = []
    async for batch in producer.produce():
        out.append(batch)
    return out


# Strategies
chunk_sizes_strategy = st.lists(
    st.integers(min_value=0, max_value=200),
    min_size=0,
    max_size=20,
)
batch_size_strategy = st.integers(min_value=1, max_value=300)


@given(chunk_sizes=chunk_sizes_strategy, batch_size=batch_size_strategy)
@settings(max_examples=200, deadline=2000)
def test_property_record_conservation(chunk_sizes, batch_size):
    """The total number of records yielded downstream equals the total number
    yielded by produce_chunks. No records lost; none duplicated."""
    p = _ScriptedProducer({"batch_size": batch_size}, chunk_sizes=chunk_sizes)
    batches = asyncio.run(_drain(p))
    expected_total = sum(chunk_sizes)
    actual_total = sum(len(b) for b in batches)
    assert actual_total == expected_total, \
        f"chunk_sizes={chunk_sizes}, batch_size={batch_size}: " \
        f"expected {expected_total} records, got {actual_total}"


@given(chunk_sizes=chunk_sizes_strategy, batch_size=batch_size_strategy)
@settings(max_examples=200, deadline=2000)
def test_property_record_order_preserved(chunk_sizes, batch_size):
    """Records flow downstream in the same order produce_chunks emits them.
    Re-chunking doesn't shuffle."""
    p = _ScriptedProducer({"batch_size": batch_size}, chunk_sizes=chunk_sizes)
    batches = asyncio.run(_drain(p))
    flat = [r["v"] for b in batches for r in b]
    expected = list(range(sum(chunk_sizes)))
    assert flat == expected, \
        f"chunk_sizes={chunk_sizes}, batch_size={batch_size}: order mismatch"


@given(chunk_sizes=chunk_sizes_strategy, batch_size=batch_size_strategy)
@settings(max_examples=200, deadline=2000)
def test_property_batch_sizes_well_formed(chunk_sizes, batch_size):
    """Every batch is non-empty AND has length ≤ batch_size. All batches except
    possibly the last have length == batch_size (the last may be partial on EOS)."""
    p = _ScriptedProducer({"batch_size": batch_size}, chunk_sizes=chunk_sizes)
    batches = asyncio.run(_drain(p))
    for i, b in enumerate(batches):
        assert len(b) > 0, f"batch {i} is empty: {batches}"
        assert len(b) <= batch_size, f"batch {i} exceeds batch_size: {len(b)} > {batch_size}"
    # All non-final batches should be exactly batch_size (no time-based flush
    # here since flush_ms is not set).
    for i, b in enumerate(batches[:-1]):
        assert len(b) == batch_size, \
            f"batch {i} is partial mid-stream: len={len(b)}, batch_size={batch_size}"


@given(chunk_sizes=chunk_sizes_strategy, batch_size=batch_size_strategy)
@settings(max_examples=200, deadline=2000)
def test_property_no_empty_emissions(chunk_sizes, batch_size):
    """If produce_chunks emits empty chunks, the base class doesn't propagate
    them downstream."""
    # Inject empty chunks throughout the sequence.
    chunks_with_empties = []
    for size in chunk_sizes:
        chunks_with_empties.append(0)  # empty
        chunks_with_empties.append(size)
    p = _ScriptedProducer({"batch_size": batch_size}, chunk_sizes=chunks_with_empties)
    batches = asyncio.run(_drain(p))
    for i, b in enumerate(batches):
        assert len(b) > 0, f"empty batch emitted at index {i}"


@given(num_records=st.integers(min_value=0, max_value=500),
       batch_size=st.integers(min_value=1, max_value=100))
@settings(max_examples=100, deadline=2000)
def test_property_partial_final_batch_only(num_records, batch_size):
    """When all records come in one big chunk, the output is N full batches plus
    optionally one partial batch — never a partial in the middle."""
    p = _ScriptedProducer({"batch_size": batch_size}, chunk_sizes=[num_records])
    batches = asyncio.run(_drain(p))
    if num_records == 0:
        assert batches == [], "expected no batches for empty source"
        return
    expected_full, remainder = divmod(num_records, batch_size)
    sizes = [len(b) for b in batches]
    if remainder == 0:
        assert sizes == [batch_size] * expected_full
    else:
        assert sizes == [batch_size] * expected_full + [remainder]
