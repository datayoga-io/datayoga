# Producer Batching Unification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move batching out of individual producer blocks into the `Producer` base class so every read block has consistent `batch_size` behavior, and three buggy producers stop yielding single records.

**Architecture:** The `Producer` base class gets a new abstract-by-convention hook `produce_chunks()` that yields lists of any size. Its `produce()` method becomes a re-chunker that emits exact `batch_size` batches, with an optional `flush_ms` timeout-flush for streaming sources. Schema fragments (`batchable.schema.json`, `streamable.schema.json`) provide the shared `batch_size`/`flush_ms` definitions, resolved at load time via a `$inherit` convention. Each of the 7 producer blocks migrates to override `produce_chunks` instead of `produce`.

**Tech Stack:** Python 3.7+, asyncio, jsonschema, pytest (asyncio mode), SQLAlchemy, redis-py, aiohttp, azure-eventhub.

**Spec:** `docs/superpowers/specs/2026-05-28-producer-batching-unification-design.md`
**Issue:** #400

---

## File Structure

**Created:**
- `core/src/datayoga_core/resources/schemas/batchable.schema.json` — fragment exposing `batch_size`
- `core/src/datayoga_core/resources/schemas/streamable.schema.json` — fragment exposing `flush_ms` (combined with batchable)
- `core/src/datayoga_core/schema_utils.py` — `$inherit` resolver used by Block + Job
- `core/src/datayoga_core/tests/__init__.py` — empty, makes the tests package importable
- `core/src/datayoga_core/tests/test_schema_inherit.py` — tests for the `$inherit` resolver
- `core/src/datayoga_core/tests/test_producer_batching.py` — base-class batching tests
- `core/src/datayoga_core/blocks/parquet/read/tests/__init__.py` (if package missing)
- `core/src/datayoga_core/blocks/parquet/read/tests/test_parquet_read.py`
- `core/src/datayoga_core/blocks/files/read_csv/tests/__init__.py`
- `core/src/datayoga_core/blocks/files/read_csv/tests/test_read_csv.py`
- `core/src/datayoga_core/blocks/redis/read_stream/tests/__init__.py`
- `core/src/datayoga_core/blocks/redis/read_stream/tests/test_redis_read_stream.py`
- `core/src/datayoga_core/blocks/http/receiver/tests/__init__.py`
- `core/src/datayoga_core/blocks/http/receiver/tests/test_http_receiver.py`
- `core/src/datayoga_core/blocks/azure/read_event_hub/tests/__init__.py`
- `core/src/datayoga_core/blocks/azure/read_event_hub/tests/test_event_hub.py`
- `core/src/datayoga_core/blocks/relational/read/tests/__init__.py`
- `core/src/datayoga_core/blocks/relational/read/tests/test_relational_read.py`

**Modified:**
- `core/src/datayoga_core/producer.py` — adds `produce_chunks` and a default `produce()` that re-chunks
- `core/src/datayoga_core/block.py` — `get_json_schema()` runs through `$inherit` resolver
- `core/src/datayoga_core/job.py` — `get_json_schema()` loop runs each loaded schema through the resolver
- `core/src/datayoga_core/blocks/std/read/block.py` — replace `process_batch` with `produce_chunks`
- `core/src/datayoga_core/blocks/std/read/block.schema.json` — use `$inherit: ["batchable"]`
- `core/src/datayoga_core/blocks/files/read_csv/block.py` — `produce_chunks` (drop `islice` loop in `produce`)
- `core/src/datayoga_core/blocks/files/read_csv/block.schema.json` — drop inline `batch_size`, add `$inherit`
- `core/src/datayoga_core/blocks/parquet/read/block.py` — `produce_chunks` per row group
- `core/src/datayoga_core/blocks/parquet/read/block.schema.json` — add `$inherit`
- `core/src/datayoga_core/blocks/relational/read/block.py` — `produce_chunks` with `fetch_size`
- `core/src/datayoga_core/blocks/relational/read/block.schema.json` — add `$inherit` + `fetch_size` property
- `core/src/datayoga_core/blocks/redis/read_stream/block.py` — `produce_chunks` with `count=batch_size`
- `core/src/datayoga_core/blocks/redis/read_stream/block.schema.json` — `$inherit: ["streamable"]`
- `core/src/datayoga_core/blocks/http/receiver/block.py` — `produce_chunks` drains queue
- `core/src/datayoga_core/blocks/http/receiver/block.schema.json` — `$inherit: ["streamable"]`
- `core/src/datayoga_core/blocks/azure/read_event_hub/block.py` — `produce_chunks`, rename `batch_size` → `max_batch_size`
- `core/src/datayoga_core/blocks/azure/read_event_hub/block.schema.json` — rename property, add `additionalProperties: false`, `$inherit: ["streamable"]`
- `schemas/job.schema.json` — regenerated at the end
- `docs/reference/blocks/*.md` — regenerated at the end
- `docs/processing-strategies.md` — new section on producer batching

---

## Task 1: Schema fragment loader

Adds the `$inherit` convention and the two shared fragments. After this task, schemas referencing `batchable` / `streamable` get the fragments' properties merged in at load time.

**Files:**
- Create: `core/src/datayoga_core/resources/schemas/batchable.schema.json`
- Create: `core/src/datayoga_core/resources/schemas/streamable.schema.json`
- Create: `core/src/datayoga_core/schema_utils.py`
- Create: `core/src/datayoga_core/tests/__init__.py`
- Create: `core/src/datayoga_core/tests/test_schema_inherit.py`
- Modify: `core/src/datayoga_core/block.py` (lines 44–59)
- Modify: `core/src/datayoga_core/job.py` (lines 223–244)

- [ ] **Step 1.1: Create the `batchable` fragment**

Create `core/src/datayoga_core/resources/schemas/batchable.schema.json`:

```json
{
  "title": "batchable",
  "description": "Producer batching mixin: declares batch_size for producers that yield records in batches.",
  "type": "object",
  "properties": {
    "batch_size": {
      "type": "integer",
      "minimum": 1,
      "description": "Maximum number of records yielded per downstream batch.",
      "default": 1000
    }
  }
}
```

- [ ] **Step 1.2: Create the `streamable` fragment**

Create `core/src/datayoga_core/resources/schemas/streamable.schema.json`:

```json
{
  "title": "streamable",
  "description": "Streaming producer mixin: declares batch_size and flush_ms for producers reading from continuous sources.",
  "type": "object",
  "properties": {
    "batch_size": {
      "type": "integer",
      "minimum": 1,
      "description": "Maximum number of records yielded per downstream batch.",
      "default": 1000
    },
    "flush_ms": {
      "type": ["integer", "null"],
      "minimum": 1,
      "description": "If set, flush a partial batch after this many ms of inactivity. null or omitted = wait until batch_size or end-of-stream.",
      "default": 1000
    }
  }
}
```

- [ ] **Step 1.3: Create empty tests package**

If `core/src/datayoga_core/tests/__init__.py` does not exist, create it as an empty file. (Several test modules in this plan live in `core/src/datayoga_core/tests/`; the directory must be importable.)

```bash
test -f core/src/datayoga_core/tests/__init__.py || touch core/src/datayoga_core/tests/__init__.py
```

- [ ] **Step 1.4: Write the failing test for `$inherit` resolution**

Create `core/src/datayoga_core/tests/test_schema_inherit.py`:

```python
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
```

- [ ] **Step 1.5: Run test to verify it fails**

Run:
```bash
cd core && python -m pytest src/datayoga_core/tests/test_schema_inherit.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'datayoga_core.schema_utils'`.

- [ ] **Step 1.6: Implement the resolver**

Create `core/src/datayoga_core/schema_utils.py`:

```python
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
```

- [ ] **Step 1.7: Run test to verify it passes**

Run:
```bash
cd core && python -m pytest src/datayoga_core/tests/test_schema_inherit.py -v
```

Expected: 5 passed.

- [ ] **Step 1.8: Wire resolver into `Block.get_json_schema`**

Modify `core/src/datayoga_core/block.py`. After loading the schema (currently `return utils.read_json(json_schema_file)` on line 59), pass it through the resolver.

Replace lines 44–59 with:

```python
    def get_json_schema(self) -> Dict[str, Any]:
        """Returns the JSON Schema for this block.

        Returns:
            Dict[str, Any]: JSON Schema.
        """
        json_schema_file = path.join(
            utils.get_bundled_dir(),
            os.path.relpath(
                os.path.dirname(sys.modules[self.__module__].__file__),
                start=os.path.dirname(__file__)),
            "block.schema.json") if utils.is_bundled() else path.join(
            os.path.dirname(os.path.realpath(sys.modules[self.__module__].__file__)),
            "block.schema.json")
        logger.debug(f"loading schema from {json_schema_file}")
        from datayoga_core.schema_utils import resolve_inherits
        return resolve_inherits(utils.read_json(json_schema_file))
```

Note: the `from datayoga_core.schema_utils import resolve_inherits` line is inside the function to avoid a circular import (schema_utils imports from utils, utils imports from block).

- [ ] **Step 1.9: Wire resolver into `Job.get_json_schema`**

Modify `core/src/datayoga_core/job.py`. Inside the `for block_type, schema_path in block_info:` loop (around line 240–243), apply the resolver to each loaded schema.

Find this block:
```python
        for block_type, schema_path in block_info:
            block_types.append(block_type)
            # load schema file
            schema = utils.read_json(f"{schema_path}")
            # append to the array of allOf for the full schema
```

Replace with:
```python
        from datayoga_core.schema_utils import resolve_inherits
        for block_type, schema_path in block_info:
            block_types.append(block_type)
            # load schema file
            schema = resolve_inherits(utils.read_json(f"{schema_path}"))
            # append to the array of allOf for the full schema
```

- [ ] **Step 1.10: Verify existing block validation still passes**

Run the full core test suite to make sure nothing regressed (no producer is using `$inherit` yet, so behavior should be unchanged):

```bash
cd core && python -m pytest src/datayoga_core/ -x -q
```

Expected: all existing tests pass; the 5 new `test_schema_inherit.py` tests also pass.

- [ ] **Step 1.11: Commit**

```bash
git add core/src/datayoga_core/resources/schemas/batchable.schema.json \
        core/src/datayoga_core/resources/schemas/streamable.schema.json \
        core/src/datayoga_core/schema_utils.py \
        core/src/datayoga_core/tests/__init__.py \
        core/src/datayoga_core/tests/test_schema_inherit.py \
        core/src/datayoga_core/block.py \
        core/src/datayoga_core/job.py
git commit -m "Add \$inherit schema fragment resolver (#400)"
```

---

## Task 2: Producer base class with batching

Add `produce_chunks()` and a default `produce()` that re-chunks. Existing subclasses override `produce()` directly and are unaffected until migrated in later tasks.

**Files:**
- Create: `core/src/datayoga_core/tests/test_producer_batching.py`
- Modify: `core/src/datayoga_core/producer.py`

- [ ] **Step 2.1: Write the failing tests**

Create `core/src/datayoga_core/tests/test_producer_batching.py`:

```python
import asyncio
from typing import AsyncGenerator, List, Optional

import pytest

from datayoga_core.context import Context
from datayoga_core.producer import Message, Producer


def _msg(i: int) -> dict:
    return {Producer.MSG_ID_FIELD: str(i), "v": i}


class FakeProducer(Producer):
    """Producer driven by a scripted list of chunks plus optional sleeps."""

    def __init__(self, properties=None, *, chunks=None, sleep_before=None):
        # schema for a FakeProducer; declare batch_size/flush_ms so validation passes
        self._test_schema = {
            "type": "object",
            "properties": {
                "batch_size": {"type": "integer", "minimum": 1},
                "flush_ms": {"type": ["integer", "null"], "minimum": 1},
            },
        }
        self._chunks = chunks or []
        self._sleep_before = sleep_before or []
        super().__init__(properties or {})

    def get_json_schema(self):
        return self._test_schema

    def init(self, context: Optional[Context] = None):
        pass

    async def produce_chunks(self) -> AsyncGenerator[List[Message], None]:
        for i, chunk in enumerate(self._chunks):
            if i < len(self._sleep_before) and self._sleep_before[i]:
                await asyncio.sleep(self._sleep_before[i])
            yield chunk


async def _drain(producer: Producer):
    out = []
    async for batch in producer.produce():
        out.append(batch)
    return out


@pytest.mark.asyncio
async def test_rechunks_one_large_chunk():
    chunks = [[_msg(i) for i in range(5000)]]
    p = FakeProducer({"batch_size": 1000}, chunks=chunks)
    batches = await _drain(p)
    assert [len(b) for b in batches] == [1000, 1000, 1000, 1000, 1000]


@pytest.mark.asyncio
async def test_accumulates_small_chunks_and_flushes_on_eos():
    chunks = [[_msg(i) for i in range(200)],
              [_msg(i) for i in range(200, 500)],
              [_msg(i) for i in range(500, 900)]]
    p = FakeProducer({"batch_size": 1000}, chunks=chunks)
    batches = await _drain(p)
    assert [len(b) for b in batches] == [900]


@pytest.mark.asyncio
async def test_partial_final_batch_on_eos():
    chunks = [[_msg(i) for i in range(1500)]]
    p = FakeProducer({"batch_size": 1000}, chunks=chunks)
    batches = await _drain(p)
    assert [len(b) for b in batches] == [1000, 500]


@pytest.mark.asyncio
async def test_empty_chunks_are_ignored():
    chunks = [[], [_msg(1), _msg(2)], [], [_msg(3)]]
    p = FakeProducer({"batch_size": 10}, chunks=chunks)
    batches = await _drain(p)
    assert [len(b) for b in batches] == [3]


@pytest.mark.asyncio
async def test_flush_ms_emits_partial_on_inactivity():
    # one chunk of 2 records, then a 300ms wait before EOS; flush_ms=100 should
    # flush the partial batch of 2 well before EOS.
    chunks = [[_msg(1), _msg(2)], [_msg(3)]]
    sleeps = [0, 0.3]
    p = FakeProducer({"batch_size": 100, "flush_ms": 100},
                     chunks=chunks, sleep_before=sleeps)

    received = []
    started = asyncio.get_event_loop().time()
    timings = []
    async for batch in p.produce():
        timings.append(asyncio.get_event_loop().time() - started)
        received.append(batch)

    assert [len(b) for b in received] == [2, 1]
    # first flush happens because of inactivity (~100ms), not waiting for chunk 2
    assert timings[0] < 0.25, f"expected first flush before 250ms, got {timings[0]}"


@pytest.mark.asyncio
async def test_no_flush_ms_holds_records_until_eos():
    chunks = [[_msg(1)], [_msg(2)]]
    sleeps = [0, 0.1]
    p = FakeProducer({"batch_size": 100}, chunks=chunks, sleep_before=sleeps)
    batches = await _drain(p)
    assert [len(b) for b in batches] == [2]  # combined on EOS, never flushed mid-stream


@pytest.mark.asyncio
async def test_consumer_cancellation_cleans_up_pump():
    chunks = [[_msg(i)] for i in range(1000)]
    p = FakeProducer({"batch_size": 10, "flush_ms": 50}, chunks=chunks,
                     sleep_before=[0.05] * 1000)

    gen = p.produce()
    first = await gen.__anext__()
    assert len(first) >= 1
    await gen.aclose()
    # If pump task wasn't cleaned up we'd see a "Task was destroyed but it is
    # pending!" warning here. Sleep briefly so the loop has a chance to surface it.
    await asyncio.sleep(0.1)
```

- [ ] **Step 2.2: Run tests to verify they fail**

Run:
```bash
cd core && python -m pytest src/datayoga_core/tests/test_producer_batching.py -v
```

Expected: All 7 tests FAIL with `TypeError: Can't instantiate abstract class FakeProducer with abstract methods produce` (because `produce` is currently abstract and `FakeProducer` doesn't override it; it overrides `produce_chunks` which doesn't exist yet).

- [ ] **Step 2.3: Implement the new `Producer` base class**

Replace the contents of `core/src/datayoga_core/producer.py` with:

```python
import asyncio
import logging
from contextlib import suppress
from typing import Any, AsyncGenerator, Dict, List

from .block import Block

logger = logging.getLogger("dy")


class Message:
    def __init__(self, msg_id: str, value: Dict[str, Any]):
        self.msg_id = msg_id
        self.value = value


class Producer(Block):
    """Base class for producer (read) blocks.

    Subclasses override `produce_chunks()` to yield chunks of any size from
    the source. The default `produce()` re-chunks them to exactly `batch_size`
    records per batch (smaller on flush_ms timeout or end-of-stream).

    Legacy subclasses may still override `produce()` directly. They bypass
    the base-class batching and `produce_chunks` is not called.
    """

    DEFAULT_BATCH_SIZE = 1000
    DEFAULT_FLUSH_MS = None  # streaming subclasses override to enable timeout flush

    async def produce_chunks(self) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """Yield natural-size chunks from the source.

        Subclasses should override this method. The base-class `produce()`
        will re-chunk the output to exact `batch_size` slices.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must override produce_chunks() or produce()"
        )
        # Make this an async generator for type-checking purposes.
        yield  # pragma: no cover

    async def produce(self) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """Re-chunks `produce_chunks()` output to exact batch_size batches.

        Reads `batch_size` and `flush_ms` from properties lazily so subclasses
        don't need to remember to call `super().init()`.
        """
        batch_size = int(self.properties.get("batch_size", self.DEFAULT_BATCH_SIZE))
        flush_ms = self.properties.get("flush_ms", self.DEFAULT_FLUSH_MS)
        timeout = (flush_ms / 1000) if flush_ms else None

        queue: asyncio.Queue = asyncio.Queue()
        EOS = object()

        async def pump():
            try:
                async for chunk in self.produce_chunks():
                    if chunk:
                        await queue.put(chunk)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("produce_chunks raised; ending stream: %s", exc)
            finally:
                await queue.put(EOS)

        pump_task = asyncio.create_task(pump())
        buffer: List[Dict[str, Any]] = []
        try:
            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=timeout)
                except asyncio.TimeoutError:
                    if buffer:
                        yield buffer
                        buffer = []
                    continue

                if item is EOS:
                    if buffer:
                        yield buffer
                    return

                buffer.extend(item)
                while len(buffer) >= batch_size:
                    yield buffer[:batch_size]
                    buffer = buffer[batch_size:]
        finally:
            pump_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await pump_task

    def ack(self, msg_ids: List[str]):
        """Sends acknowledge for the message IDs of records that have been processed."""
        pass
```

Key differences from the current file:
- `produce()` is no longer `@abstractmethod` — it has a default implementation.
- `produce_chunks()` is the new override hook (not formally `@abstractmethod` so legacy subclasses still validate).
- `Message` class unchanged.

- [ ] **Step 2.4: Run tests to verify they pass**

Run:
```bash
cd core && python -m pytest src/datayoga_core/tests/test_producer_batching.py -v
```

Expected: 7 passed.

- [ ] **Step 2.5: Run the full core test suite to confirm no regressions**

Existing producers all still override `produce()`, so their behavior is unchanged.

```bash
cd core && python -m pytest src/datayoga_core/ -x -q
```

Expected: all tests pass (including the new `test_producer_batching` and `test_schema_inherit`).

- [ ] **Step 2.6: Commit**

```bash
git add core/src/datayoga_core/producer.py \
        core/src/datayoga_core/tests/test_producer_batching.py
git commit -m "Producer base class re-chunks via produce_chunks (#400)"
```

---

## Task 3: Migrate `std/read`

`std/read` already has `batch_size` and a custom `process_batch` accumulator. Replace it with a `produce_chunks` that yields one chunk; the base class re-chunks.

**Files:**
- Modify: `core/src/datayoga_core/blocks/std/read/block.py`
- Modify: `core/src/datayoga_core/blocks/std/read/block.schema.json`

- [ ] **Step 3.1: Write the failing test**

There is no existing `tests/` directory under `std/read`. The std/read producer is exercised indirectly by integration tests, but we add a unit test for batching here.

Create `core/src/datayoga_core/blocks/std/read/tests/__init__.py` (empty file) and `core/src/datayoga_core/blocks/std/read/tests/test_std_read.py`:

```python
import asyncio
from unittest.mock import patch

import orjson
import pytest

from datayoga_core.blocks.std.read.block import Block


async def _drain(producer):
    out = []
    async for batch in producer.produce():
        out.append(batch)
    return out


@pytest.mark.asyncio
async def test_std_read_batches_to_batch_size():
    payload = [{"i": i} for i in range(2500)]
    fake_stdin = [orjson.dumps(payload).decode()]

    block = Block({"batch_size": 1000})
    block.init()

    with patch("datayoga_core.blocks.std.read.block.select.select",
               return_value=([object()], [], [])), \
         patch("datayoga_core.blocks.std.read.block.sys.stdin", fake_stdin):
        batches = await _drain(block)

    assert [len(b) for b in batches] == [1000, 1000, 500]
    # records carry their MSG_ID_FIELD and original payload values
    flat = [r for b in batches for r in b]
    assert flat[0]["i"] == 0
    assert all(Block.MSG_ID_FIELD in r for r in flat)
```

- [ ] **Step 3.2: Run test to verify it fails**

Run:
```bash
cd core && python -m pytest src/datayoga_core/blocks/std/read/tests/test_std_read.py -v
```

Expected: FAIL — the current implementation yields batches of `batch_size`, but its `process_batch` helper won't be exercised through the new `produce()` machinery because it overrides `produce()` directly. The test may also fail because the current produce() doesn't see the `batch_size_in_std_read_block` branch's batch logic interact cleanly with the test mocks. (The point of this step is to drive the migration; the failure shape is secondary.)

- [ ] **Step 3.3: Migrate `std/read` to `produce_chunks`**

Replace the contents of `core/src/datayoga_core/blocks/std/read/block.py` with:

```python
import logging
import select
import sys
import uuid
from typing import Any, AsyncGenerator, Dict, List, Optional

import orjson
from datayoga_core.context import Context
from datayoga_core.producer import Producer as DyProducer

logger = logging.getLogger("dy")


class Block(DyProducer):
    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")

    async def produce_chunks(self) -> AsyncGenerator[List[Dict[str, Any]], None]:
        if select.select([sys.stdin], [], [], 0.0)[0]:
            all_records: List[Dict[str, Any]] = []
            for line in sys.stdin:
                all_records.extend(self.get_records(line))
        else:
            print("Enter data to process:")
            all_records = self.get_records(input())

        if all_records:
            yield [self.get_message(record) for record in all_records]

    @staticmethod
    def get_records(data: str) -> List[Dict[str, Any]]:
        records = orjson.loads(data)
        if isinstance(records, dict):
            records = [records]
        return records

    def get_message(self, record: Dict[str, Any]) -> Dict[str, Any]:
        return {self.MSG_ID_FIELD: str(uuid.uuid4()), **record}
```

The `process_batch`, `batch_size` init read, and `produce` override are all gone. The base class handles batching.

- [ ] **Step 3.4: Update the schema to use the fragment**

Replace the contents of `core/src/datayoga_core/blocks/std/read/block.schema.json` with:

```json
{
  "title": "std.read",
  "description": "Read from the standard input",
  "type": "object",
  "$inherit": ["batchable"],
  "properties": {},
  "additionalProperties": false
}
```

The `batch_size` declaration now comes from the fragment.

- [ ] **Step 3.5: Run test to verify it passes**

```bash
cd core && python -m pytest src/datayoga_core/blocks/std/read/tests/test_std_read.py -v
```

Expected: PASS.

- [ ] **Step 3.6: Run the full core suite**

```bash
cd core && python -m pytest src/datayoga_core/ -x -q
```

Expected: all tests pass.

- [ ] **Step 3.7: Commit**

```bash
git add core/src/datayoga_core/blocks/std/read/block.py \
        core/src/datayoga_core/blocks/std/read/block.schema.json \
        core/src/datayoga_core/blocks/std/read/tests/__init__.py \
        core/src/datayoga_core/blocks/std/read/tests/test_std_read.py
git commit -m "Migrate std/read to produce_chunks (#400, #296)"
```

---

## Task 4: Migrate `files/read_csv`

Replace the `produce()` override and `islice` loop with a `produce_chunks` that yields one chunk per `batch_size` rows. The base class re-chunks to the configured `batch_size`.

**Files:**
- Modify: `core/src/datayoga_core/blocks/files/read_csv/block.py`
- Modify: `core/src/datayoga_core/blocks/files/read_csv/block.schema.json`

- [ ] **Step 4.1: Write the failing test**

Create `core/src/datayoga_core/blocks/files/read_csv/tests/__init__.py` (empty) and `core/src/datayoga_core/blocks/files/read_csv/tests/test_read_csv.py`:

```python
from pathlib import Path

import pytest

from datayoga_core.blocks.files.read_csv.block import Block


async def _drain(producer):
    out = []
    async for batch in producer.produce():
        out.append(batch)
    return out


@pytest.fixture
def csv_path(tmp_path) -> Path:
    p = tmp_path / "data.csv"
    rows = ["fname,lname"] + [f"first{i},last{i}" for i in range(2500)]
    p.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return p


@pytest.mark.asyncio
async def test_csv_batches_to_batch_size(csv_path):
    block = Block({"file": str(csv_path), "batch_size": 1000, "skip": 1})
    block.init()
    batches = await _drain(block)
    assert [len(b) for b in batches] == [1000, 1000, 500]
    # message ids are populated
    assert all(Block.MSG_ID_FIELD in r for b in batches for r in b)
    # first row content
    assert batches[0][0]["fname"] == "first0"


@pytest.mark.asyncio
async def test_csv_default_batch_size(csv_path):
    block = Block({"file": str(csv_path), "skip": 1})
    block.init()
    batches = await _drain(block)
    # default batch_size is 1000
    assert [len(b) for b in batches] == [1000, 1000, 500]
```

- [ ] **Step 4.2: Run test to verify it fails**

```bash
cd core && python -m pytest src/datayoga_core/blocks/files/read_csv/tests/test_read_csv.py -v
```

Expected: FAIL — current `produce()` works but the tests may pass coincidentally because `files/read_csv` already batches. That's fine; the test exists to *protect* the contract. Proceed to the migration anyway and confirm the test still passes afterward.

- [ ] **Step 4.3: Migrate `files/read_csv` to `produce_chunks`**

Replace the contents of `core/src/datayoga_core/blocks/files/read_csv/block.py` with:

```python
import logging
import os
from abc import ABCMeta
from contextlib import suppress
from csv import DictReader
from itertools import count, islice
from typing import Any, AsyncGenerator, Dict, List, Optional

from datayoga_core.context import Context
from datayoga_core.producer import Producer as DyProducer

logger = logging.getLogger("dy")


class Block(DyProducer, metaclass=ABCMeta):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        csv_file = self.properties["file"]
        if os.path.isabs(csv_file) or context is None:
            self.file = csv_file
        else:
            self.file = os.path.join(context.properties.get("data_path"), csv_file)
        logger.debug(f"file: {self.file}")
        self.encoding = self.properties.get("encoding", "utf-8")
        self.fields = self.properties.get("fields")
        self.skip = self.properties.get("skip", 0)
        self.delimiter = self.properties.get("delimiter", ",")
        self.quotechar = self.properties.get("quotechar", "\"")

    async def produce_chunks(self) -> AsyncGenerator[List[Dict[str, Any]], None]:
        logger.debug("Reading CSV")
        batch_size = int(self.properties.get("batch_size", self.DEFAULT_BATCH_SIZE))

        with open(self.file, "r", encoding=self.encoding) as read_obj:
            reader = DictReader(read_obj, fieldnames=self.fields,
                                delimiter=self.delimiter, quotechar=self.quotechar)
            for _ in range(self.skip):
                with suppress(StopIteration):
                    next(reader)
            counter = iter(count())
            while True:
                chunk = [
                    {self.MSG_ID_FIELD: f"{next(counter)}", **record}
                    for record in islice(reader, batch_size)
                ]
                if not chunk:
                    return
                yield chunk
```

The init no longer reads `self.batch_size` (read lazily in `produce_chunks`).

- [ ] **Step 4.4: Update the schema**

Replace `core/src/datayoga_core/blocks/files/read_csv/block.schema.json` with:

```json
{
  "title": "files.read_csv",
  "description": "Read data from CSV",
  "type": "object",
  "$inherit": ["batchable"],
  "properties": {
    "file": {
      "description": "Filename. Can contain a regexp or glob expression",
      "type": "string"
    },
    "encoding": {
      "description": "Encoding to use for reading the file",
      "type": "string",
      "default": "utf-8"
    },
    "fields": {
      "type": "array",
      "title": "List of columns to use",
      "description": "List of columns to use for extract",
      "default": null,
      "examples": [["fname", "lname"]],
      "minLength": 1,
      "additionalItems": true,
      "items": {
        "type": "string",
        "description": "field name",
        "examples": ["fname"]
      }
    },
    "skip": {
      "description": "Number of lines to skip",
      "type": "number",
      "minimum": 0,
      "default": 0
    },
    "delimiter": {
      "description": "Delimiter to use for splitting the csv records",
      "type": "string",
      "minLength": 1,
      "maxLength": 1,
      "default": ","
    },
    "quotechar": {
      "description": "A one-character string used to quote fields containing special characters, such as the delimiter or quotechar, or which contain new-line characters. It defaults to '",
      "type": "string",
      "minLength": 1,
      "maxLength": 1,
      "default": "\""
    }
  },
  "additionalProperties": false,
  "required": ["file"],
  "examples": [
    {
      "file": "archive.csv",
      "delimiter": ";"
    }
  ]
}
```

The `batch_size` inline property is removed; it comes from the `batchable` fragment.

- [ ] **Step 4.5: Run test to verify it passes**

```bash
cd core && python -m pytest src/datayoga_core/blocks/files/read_csv/tests/test_read_csv.py -v
```

Expected: 2 passed.

- [ ] **Step 4.6: Run the full core suite**

```bash
cd core && python -m pytest src/datayoga_core/ -x -q
```

Expected: all tests pass.

- [ ] **Step 4.7: Commit**

```bash
git add core/src/datayoga_core/blocks/files/read_csv/block.py \
        core/src/datayoga_core/blocks/files/read_csv/block.schema.json \
        core/src/datayoga_core/blocks/files/read_csv/tests/__init__.py \
        core/src/datayoga_core/blocks/files/read_csv/tests/test_read_csv.py
git commit -m "Migrate files/read_csv to produce_chunks (#400)"
```

---

## Task 5: Migrate `parquet/read` (fixes one-by-one bug)

Today `parquet/read` iterates each row of each row group and yields a single-record list per iteration. Migrate it to yield each row group as a single chunk; the base class re-chunks to `batch_size`.

**Files:**
- Modify: `core/src/datayoga_core/blocks/parquet/read/block.py`
- Modify: `core/src/datayoga_core/blocks/parquet/read/block.schema.json`

- [ ] **Step 5.1: Write the failing test**

Create `core/src/datayoga_core/blocks/parquet/read/tests/__init__.py` (empty) and `core/src/datayoga_core/blocks/parquet/read/tests/test_parquet_read.py`:

```python
from pathlib import Path

import pandas as pd
import pytest

from datayoga_core.blocks.parquet.read.block import Block


async def _drain(producer):
    out = []
    async for batch in producer.produce():
        out.append(batch)
    return out


@pytest.fixture
def parquet_path(tmp_path) -> Path:
    p = tmp_path / "data.parquet"
    df = pd.DataFrame({"i": list(range(2500))})
    # row_group_offsets=1000 creates 3 row groups (1000, 1000, 500)
    from fastparquet import write as fp_write
    fp_write(str(p), df, row_group_offsets=1000)
    return p


@pytest.mark.asyncio
async def test_parquet_batches_to_batch_size(parquet_path):
    block = Block({"file": str(parquet_path), "batch_size": 1000})
    block.init()
    batches = await _drain(block)
    assert [len(b) for b in batches] == [1000, 1000, 500]
    flat = [r for b in batches for r in b]
    assert flat[0]["i"] == 0
    assert all(Block.MSG_ID_FIELD in r for r in flat)


@pytest.mark.asyncio
async def test_parquet_rechunks_across_row_groups(parquet_path):
    # row groups are [1000, 1000, 500]; batch_size=750 should give batches of
    # [750, 750, 750, 250] regardless of row group boundaries.
    block = Block({"file": str(parquet_path), "batch_size": 750})
    block.init()
    batches = await _drain(block)
    assert [len(b) for b in batches] == [750, 750, 750, 250]
```

- [ ] **Step 5.2: Run test to verify it fails**

```bash
cd core && python -m pytest src/datayoga_core/blocks/parquet/read/tests/test_parquet_read.py -v
```

Expected: FAIL — current implementation yields batches of size 1, so the assertions fail.

- [ ] **Step 5.3: Migrate `parquet/read`**

Replace the contents of `core/src/datayoga_core/blocks/parquet/read/block.py` with:

```python
import logging
import os
from abc import ABCMeta
from itertools import count
from typing import Any, AsyncGenerator, Dict, List, Optional

from datayoga_core.context import Context
from datayoga_core.producer import Producer as DyProducer
from fastparquet import ParquetFile

logger = logging.getLogger("dy")


class Block(DyProducer, metaclass=ABCMeta):

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        parquet_file = self.properties["file"]
        if os.path.isabs(parquet_file) or context is None:
            self.file = parquet_file
        else:
            self.file = os.path.join(context.properties.get("data_path"), parquet_file)
        logger.debug(f"file: {self.file}")

    async def produce_chunks(self) -> AsyncGenerator[List[Dict[str, Any]], None]:
        logger.debug("Reading parquet")
        pf = ParquetFile(self.file)
        counter = iter(count())
        for df in pf.iter_row_groups():
            yield [
                {self.MSG_ID_FIELD: str(next(counter)), **row.to_dict()}
                for _, row in df.iterrows()
            ]
```

- [ ] **Step 5.4: Update the schema**

Replace `core/src/datayoga_core/blocks/parquet/read/block.schema.json` with:

```json
{
  "title": "parquet.read",
  "description": "Read data from parquet",
  "type": "object",
  "$inherit": ["batchable"],
  "properties": {
    "file": {
      "description": "Filename. Can contain a regexp or glob expression",
      "type": "string"
    }
  },
  "additionalProperties": false,
  "required": ["file"],
  "examples": [
    {
      "file": "data.parquet"
    }
  ]
}
```

- [ ] **Step 5.5: Run test to verify it passes**

```bash
cd core && python -m pytest src/datayoga_core/blocks/parquet/read/tests/test_parquet_read.py -v
```

Expected: 2 passed.

- [ ] **Step 5.6: Run the full core suite**

```bash
cd core && python -m pytest src/datayoga_core/ -x -q
```

Expected: all tests pass.

- [ ] **Step 5.7: Commit**

```bash
git add core/src/datayoga_core/blocks/parquet/read/block.py \
        core/src/datayoga_core/blocks/parquet/read/block.schema.json \
        core/src/datayoga_core/blocks/parquet/read/tests/__init__.py \
        core/src/datayoga_core/blocks/parquet/read/tests/test_parquet_read.py
git commit -m "Migrate parquet/read to produce_chunks, fix one-by-one yield (#400, #293)"
```

---

## Task 6: Migrate `relational/read` (fix bug + add `fetch_size`)

Today `relational/read` does `fetchmany(10000)` then yields one row at a time. Migrate to `produce_chunks` that yields each `fetchmany` result. Add an optional `fetch_size` property; default to 10000 to preserve today's DB round-trip count.

**Files:**
- Modify: `core/src/datayoga_core/blocks/relational/read/block.py`
- Modify: `core/src/datayoga_core/blocks/relational/read/block.schema.json`

- [ ] **Step 6.1: Write the failing test**

Create `core/src/datayoga_core/blocks/relational/read/tests/__init__.py` (empty) and `core/src/datayoga_core/blocks/relational/read/tests/test_relational_read.py`:

```python
from unittest.mock import MagicMock, patch

import pytest

from datayoga_core.blocks.relational.read.block import Block


async def _drain(producer):
    out = []
    async for batch in producer.produce():
        out.append(batch)
    return out


def _fake_result(rows):
    """Build a fake SQLAlchemy result that returns rows in fetchmany chunks."""
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
    def __init__(self, d):
        self._d = d

    def _asdict(self):
        return self._d


@pytest.mark.asyncio
async def test_relational_read_yields_batches_not_rows():
    rows = [_Row({"i": i}) for i in range(2500)]
    fake_result = _fake_result(rows)

    block = Block.__new__(Block)
    block.properties = {"batch_size": 1000}
    block.connection = MagicMock()
    block.tbl = MagicMock()
    block.tbl.select.return_value = "SELECT *"
    block.connection.execution_options.return_value.execute.return_value = fake_result

    batches = await _drain(block)
    assert [len(b) for b in batches] == [1000, 1000, 500]


@pytest.mark.asyncio
async def test_relational_read_fetch_size_independent_of_batch_size():
    rows = [_Row({"i": i}) for i in range(5000)]
    fake_result = _fake_result(rows)

    block = Block.__new__(Block)
    block.properties = {"batch_size": 1000, "fetch_size": 2500}
    block.connection = MagicMock()
    block.tbl = MagicMock()
    block.tbl.select.return_value = "SELECT *"
    block.connection.execution_options.return_value.execute.return_value = fake_result

    batches = await _drain(block)
    # Downstream batches are still batch_size=1000
    assert [len(b) for b in batches] == [1000, 1000, 1000, 1000, 1000]
    # Driver fetched in fetch_size=2500 chunks: 2500 + 2500 + 0 = 3 calls
    fetch_sizes = [c.args[0] for c in fake_result.fetchmany.call_args_list]
    assert fetch_sizes[0] == 2500
    assert fetch_sizes[1] == 2500


@pytest.mark.asyncio
async def test_relational_read_default_fetch_size_is_10000():
    rows = [_Row({"i": i}) for i in range(500)]
    fake_result = _fake_result(rows)

    block = Block.__new__(Block)
    block.properties = {}
    block.connection = MagicMock()
    block.tbl = MagicMock()
    block.tbl.select.return_value = "SELECT *"
    block.connection.execution_options.return_value.execute.return_value = fake_result

    await _drain(block)
    fetch_sizes = [c.args[0] for c in fake_result.fetchmany.call_args_list]
    assert fetch_sizes[0] == 10000
```

- [ ] **Step 6.2: Run test to verify it fails**

```bash
cd core && python -m pytest src/datayoga_core/blocks/relational/read/tests/test_relational_read.py -v
```

Expected: FAIL — the current `produce()` yields one row at a time, so `[len(b) for b in batches]` is `[1] * 2500`.

- [ ] **Step 6.3: Migrate `relational/read`**

Replace the contents of `core/src/datayoga_core/blocks/relational/read/block.py` with:

```python
import logging
from typing import Any, AsyncGenerator, Dict, List, Optional

import sqlalchemy as sa
from datayoga_core import utils
from datayoga_core.blocks.relational import utils as relational_utils
from datayoga_core.context import Context
from datayoga_core.producer import Producer as DyProducer

logger = logging.getLogger("dy")


class Block(DyProducer):
    DEFAULT_FETCH_SIZE = 10000

    def init(self, context: Optional[Context] = None):
        self.engine, self.db_type = relational_utils.get_engine(
            self.properties["connection"],
            context,
            autocommit=False,
        )

        self.schema = self.properties.get("schema")
        self.table = self.properties.get("table")
        self.opcode_field = self.properties.get("opcode_field")
        self.load_strategy = self.properties.get("load_strategy")
        self.keys = self.properties.get("keys")
        self.mapping = self.properties.get("mapping")

        self.tbl = sa.Table(self.table, sa.MetaData(schema=self.schema), autoload_with=self.engine)

        logger.debug(f"Connecting to {self.db_type}")
        self.connection = self.engine.connect()

    async def produce_chunks(self) -> AsyncGenerator[List[Dict[str, Any]], None]:
        fetch_size = int(self.properties.get("fetch_size", self.DEFAULT_FETCH_SIZE))
        result = self.connection.execution_options(stream_results=True).execute(self.tbl.select())
        while True:
            rows = result.fetchmany(fetch_size)
            if not rows:
                return
            yield [utils.add_uid(dict(row._asdict())) for row in rows]

    def stop(self):
        self.connection.close()
        self.engine.dispose()
```

- [ ] **Step 6.4: Update the schema**

Replace `core/src/datayoga_core/blocks/relational/read/block.schema.json` with:

```json
{
  "title": "relational.read",
  "description": "Read a table from an SQL-compatible data store",
  "type": "object",
  "$inherit": ["batchable"],
  "additionalProperties": false,
  "examples": [
    {
      "id": "read_snowflake",
      "type": "relational.read",
      "properties": {
        "connection": "eu_datalake",
        "table": "employees",
        "schema": "dbo"
      }
    }
  ],
  "properties": {
    "connection": {
      "type": "string",
      "title": "The connection to use for loading",
      "description": "Logical connection name as defined in the connections.dy.yaml",
      "examples": ["europe_db", "target", "eu_dwh"]
    },
    "schema": {
      "type": "string",
      "title": "The table schema of the table",
      "description": "If left blank, the default schema of this connection will be used as defined in the connections.dy.yaml",
      "examples": ["dbo"]
    },
    "table": {
      "type": "string",
      "title": "The table name",
      "description": "Table name",
      "examples": ["employees"]
    },
    "columns": {
      "type": "array",
      "title": "Optional subset of columns to load",
      "items": {
        "type": ["string", "object"],
        "title": "name of column"
      },
      "examples": [["fname", { "lname": "last_name" }]]
    },
    "fetch_size": {
      "type": "integer",
      "minimum": 1,
      "description": "Driver-level rows fetched per round-trip. Defaults to 10000.",
      "default": 10000
    }
  },
  "required": ["connection", "table"]
}
```

- [ ] **Step 6.5: Run test to verify it passes**

```bash
cd core && python -m pytest src/datayoga_core/blocks/relational/read/tests/test_relational_read.py -v
```

Expected: 3 passed.

- [ ] **Step 6.6: Run the full core suite**

```bash
cd core && python -m pytest src/datayoga_core/ -x -q
```

Expected: all tests pass.

- [ ] **Step 6.7: Commit**

```bash
git add core/src/datayoga_core/blocks/relational/read/block.py \
        core/src/datayoga_core/blocks/relational/read/block.schema.json \
        core/src/datayoga_core/blocks/relational/read/tests/__init__.py \
        core/src/datayoga_core/blocks/relational/read/tests/test_relational_read.py
git commit -m "Migrate relational/read to produce_chunks, add fetch_size (#400, #295)"
```

---

## Task 7: Migrate `http/receiver` (fix one-by-one)

The receiver currently yields one record per HTTP request. Migrate to drain the queue per chunk; `flush_ms` ensures partial batches flush during low-traffic periods.

**Files:**
- Modify: `core/src/datayoga_core/blocks/http/receiver/block.py`
- Modify: `core/src/datayoga_core/blocks/http/receiver/block.schema.json`

- [ ] **Step 7.1: Write the failing test**

Create `core/src/datayoga_core/blocks/http/receiver/tests/__init__.py` (empty) and `core/src/datayoga_core/blocks/http/receiver/tests/test_http_receiver.py`:

```python
import asyncio

import aiohttp
import pytest

from datayoga_core.blocks.http.receiver.block import Block


def _free_port():
    import socket
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.mark.asyncio
async def test_http_receiver_batches_incoming_requests():
    port = _free_port()
    block = Block({"host": "127.0.0.1", "port": port,
                   "batch_size": 50, "flush_ms": 200})
    block.init()

    received = []

    async def consumer():
        async for batch in block.produce():
            received.append(batch)
            if sum(len(b) for b in received) >= 60:
                return

    consumer_task = asyncio.create_task(consumer())
    await asyncio.sleep(0.2)  # let server start

    async with aiohttp.ClientSession() as session:
        for i in range(60):
            async with session.post(f"http://127.0.0.1:{port}", json={"i": i}) as r:
                assert r.status == 200

    await asyncio.wait_for(consumer_task, timeout=5)

    flat = [r for b in received for r in b]
    assert len(flat) == 60
    # Most records arrive in a full batch_size=50 batch; the rest arrive as a
    # partial batch flushed by flush_ms.
    assert any(len(b) == 50 for b in received)
    assert all(Block.MSG_ID_FIELD in r for r in flat)
```

- [ ] **Step 7.2: Run test to verify it fails**

```bash
cd core && python -m pytest src/datayoga_core/blocks/http/receiver/tests/test_http_receiver.py -v
```

Expected: FAIL — current implementation yields one record per batch; `assert any(len(b) == 50 ...)` is false.

- [ ] **Step 7.3: Migrate `http/receiver`**

Replace the contents of `core/src/datayoga_core/blocks/http/receiver/block.py` with:

```python
import logging
from abc import ABCMeta
from asyncio import Queue
from contextlib import suppress
from itertools import count
from typing import Any, AsyncGenerator, Dict, List, Optional

import orjson
from aiohttp.web import (BaseRequest, HTTPInternalServerError, HTTPOk,
                         Response, Server, ServerRunner, TCPSite)
from datayoga_core.context import Context
from datayoga_core.producer import Producer as DyProducer

logger = logging.getLogger("dy")


class Block(DyProducer, metaclass=ABCMeta):
    port: int
    host: str
    DEFAULT_FLUSH_MS = 1000

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.port = int(self.properties.get("port", 8080))
        self.host = self.properties.get("host", "0.0.0.0")

    async def produce_chunks(self) -> AsyncGenerator[List[Dict[str, Any]], None]:
        queue: Queue = Queue(maxsize=1000)

        async def handler(request: BaseRequest) -> Response:
            try:
                queue.put_nowait(orjson.loads(await request.read()))
                return HTTPOk()
            except Exception:
                logger.exception("Got exception while parsing request:")
                return HTTPInternalServerError()

        runner = ServerRunner(Server(handler))
        await runner.setup()
        srv = TCPSite(runner, self.host, self.port)
        await srv.start()
        logger.info(f"Listening on {self.host}:{self.port}...")

        try:
            counter = iter(count())
            while True:
                first = await queue.get()
                chunk = [{self.MSG_ID_FIELD: f"{next(counter)}", **first}]
                while not queue.empty():
                    record = queue.get_nowait()
                    chunk.append({self.MSG_ID_FIELD: f"{next(counter)}", **record})
                yield chunk
        finally:
            with suppress(Exception):
                await srv.stop()
```

- [ ] **Step 7.4: Update the schema**

Replace `core/src/datayoga_core/blocks/http/receiver/block.schema.json` with:

```json
{
  "title": "http.receiver",
  "description": "Receives HTTP requests and process the data.",
  "type": "object",
  "$inherit": ["streamable"],
  "properties": {
    "host": {
      "description": "Host to listen",
      "type": "string",
      "default": "0.0.0.0"
    },
    "port": {
      "description": "Port to listen",
      "type": "integer",
      "default": 8080
    }
  },
  "additionalProperties": false,
  "examples": [
    {
      "host": "localhost",
      "port": 8080
    }
  ]
}
```

- [ ] **Step 7.5: Run test to verify it passes**

```bash
cd core && python -m pytest src/datayoga_core/blocks/http/receiver/tests/test_http_receiver.py -v
```

Expected: 1 passed.

- [ ] **Step 7.6: Run the full core suite**

```bash
cd core && python -m pytest src/datayoga_core/ -x -q
```

Expected: all tests pass.

- [ ] **Step 7.7: Commit**

```bash
git add core/src/datayoga_core/blocks/http/receiver/block.py \
        core/src/datayoga_core/blocks/http/receiver/block.schema.json \
        core/src/datayoga_core/blocks/http/receiver/tests/__init__.py \
        core/src/datayoga_core/blocks/http/receiver/tests/test_http_receiver.py
git commit -m "Migrate http/receiver to produce_chunks (#400)"
```

---

## Task 8: Migrate `redis/read_stream` (closes #377)

The redis stream producer yields one record at a time today. Migrate so it requests `count=batch_size` from `xreadgroup` and yields each response as a chunk; `flush_ms` flushes partial batches during low-volume periods.

**Files:**
- Modify: `core/src/datayoga_core/blocks/redis/read_stream/block.py`
- Modify: `core/src/datayoga_core/blocks/redis/read_stream/block.schema.json`

- [ ] **Step 8.1: Write the failing test**

Create `core/src/datayoga_core/blocks/redis/read_stream/tests/__init__.py` (empty) and `core/src/datayoga_core/blocks/redis/read_stream/tests/test_redis_read_stream.py`:

```python
from unittest.mock import MagicMock

import pytest

from datayoga_core.blocks.redis.read_stream.block import Block


def _mk_block(properties, redis_client):
    block = Block.__new__(Block)
    block.properties = properties
    block.redis_client = redis_client
    block.stream = "mystream"
    block.snapshot = properties.get("_snapshot", True)
    block.consumer_group = "g"
    block.requesting_consumer = "c"
    return block


@pytest.mark.asyncio
async def test_redis_uses_count_equal_to_batch_size():
    redis = MagicMock()
    # First call returns pending messages, second call returns "no new", which
    # ends snapshot mode.
    payload_a = (b"1-0", {b"data": b'{"i": 1}'})
    payload_b = (b"2-0", {b"data": b'{"i": 2}'})
    redis.xreadgroup.side_effect = [
        [(b"mystream", [payload_a, payload_b])],  # pending
        [(b"mystream", [])],                        # nothing new -> exit
    ]

    block = _mk_block({"batch_size": 250, "_snapshot": True}, redis)
    batches = []
    async for b in block.produce():
        batches.append(b)

    assert all(c.kwargs.get("count") == 250 or (len(c.args) >= 4 and c.args[3] == 250)
               for c in redis.xreadgroup.call_args_list), \
        "xreadgroup should be called with count=batch_size"


@pytest.mark.asyncio
async def test_redis_yields_records_as_a_batch_not_one_by_one():
    redis = MagicMock()
    pages = [(f"{i}-0".encode(), {b"data": f'{{"i": {i}}}'.encode()}) for i in range(5)]
    redis.xreadgroup.side_effect = [
        [(b"mystream", pages)],
        [(b"mystream", [])],
    ]

    block = _mk_block({"batch_size": 100, "_snapshot": True}, redis)
    batches = []
    async for b in block.produce():
        batches.append(b)

    # 5 records arrive as one chunk; base class re-emits as one batch of 5.
    assert [len(b) for b in batches] == [5]
    assert batches[0][0]["i"] == 0
```

- [ ] **Step 8.2: Run test to verify it fails**

```bash
cd core && python -m pytest src/datayoga_core/blocks/redis/read_stream/tests/test_redis_read_stream.py -v
```

Expected: FAIL — current `xreadgroup` call passes `count=None`, and the producer yields one record at a time.

- [ ] **Step 8.3: Migrate `redis/read_stream`**

Replace the contents of `core/src/datayoga_core/blocks/redis/read_stream/block.py` with:

```python
import logging
from typing import Any, AsyncGenerator, Dict, List, Optional

import datayoga_core.blocks.redis.utils as redis_utils
import orjson
from datayoga_core.connection import Connection
from datayoga_core.context import Context
from datayoga_core.producer import Producer as DyProducer

logger = logging.getLogger("dy")


class Block(DyProducer):
    DEFAULT_FLUSH_MS = 1000

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        connection_details = Connection.get_connection_details(self.properties["connection"], context)
        self.redis_client = redis_utils.get_client(connection_details)
        self.stream = self.properties["stream_name"]
        self.snapshot = self.properties.get("snapshot", False)
        self.consumer_group = f'datayoga_job_{context.properties.get("job_name", "") if context else ""}'
        self.requesting_consumer = "dy_consumer_a"
        stream_groups = self.redis_client.xinfo_groups(self.stream)
        if next(filter(lambda x: x["name"] == self.consumer_group, stream_groups), None) is None:
            logger.info(f"Creating a new {self.consumer_group} consumer group associated with the {self.stream}")
            self.redis_client.xgroup_create(self.stream, self.consumer_group, 0)

    async def produce_chunks(self) -> AsyncGenerator[List[Dict[str, Any]], None]:
        logger.debug(f"Running {self.get_block_name()}")
        batch_size = int(self.properties.get("batch_size", self.DEFAULT_BATCH_SIZE))
        read_pending = True

        while True:
            streams = self.redis_client.xreadgroup(
                self.consumer_group, self.requesting_consumer,
                {self.stream: "0" if read_pending else ">"},
                count=batch_size,
                block=100 if self.snapshot else 0,
            )

            yielded_any = False
            for stream in streams:
                logger.debug(f"Messages in {self.stream} stream (pending: {read_pending}):\n\t{stream}")
                chunk: List[Dict[str, Any]] = []
                for key, value in stream[1]:
                    payload = orjson.loads(value[next(iter(value))])
                    payload[self.MSG_ID_FIELD] = key
                    chunk.append(payload)
                if chunk:
                    yielded_any = True
                    yield chunk

            # Snapshot ends after a pending-read followed by a "no new" read.
            if self.snapshot and not read_pending and not yielded_any:
                return

            read_pending = False

    def ack(self, msg_ids: List[str]):
        for msg_id in msg_ids:
            logger.info(f"Acking {msg_id} message in {self.stream} stream of {self.consumer_group} consumer group")
            self.redis_client.xack(self.stream, self.consumer_group, msg_id)
```

Note: snapshot termination is slightly tightened: the loop exits when a non-pending read returns no messages, matching the spec's intent. This is more robust than the original `if self.snapshot and not read_pending: break`.

- [ ] **Step 8.4: Update the schema**

Replace `core/src/datayoga_core/blocks/redis/read_stream/block.schema.json` with:

```json
{
  "title": "redis.read_stream",
  "description": "Read from Redis stream",
  "type": "object",
  "$inherit": ["streamable"],
  "properties": {
    "connection": { "description": "Connection name", "type": "string" },
    "stream_name": {
      "type": "string",
      "title": "Source stream name",
      "description": "Source stream name"
    },
    "snapshot": {
      "type": "boolean",
      "title": "Snapshot current entries and quit",
      "description": "Snapshot current entries and quit",
      "default": false
    }
  },
  "additionalProperties": false,
  "required": ["connection", "stream_name"]
}
```

- [ ] **Step 8.5: Run test to verify it passes**

```bash
cd core && python -m pytest src/datayoga_core/blocks/redis/read_stream/tests/test_redis_read_stream.py -v
```

Expected: 2 passed.

- [ ] **Step 8.6: Run the full core suite**

```bash
cd core && python -m pytest src/datayoga_core/ -x -q
```

Expected: all tests pass.

- [ ] **Step 8.7: Commit**

```bash
git add core/src/datayoga_core/blocks/redis/read_stream/block.py \
        core/src/datayoga_core/blocks/redis/read_stream/block.schema.json \
        core/src/datayoga_core/blocks/redis/read_stream/tests/__init__.py \
        core/src/datayoga_core/blocks/redis/read_stream/tests/test_redis_read_stream.py
git commit -m "Migrate redis/read_stream to batched xreadgroup (#400, #377)"
```

---

## Task 9: Migrate `azure/read_event_hub` (rename `batch_size` → `max_batch_size`)

Today `batch_size` controls the SDK callback size, not the pipeline batch size. Rename to `max_batch_size`, add `additionalProperties: false`, and use the streamable fragment so the *new* `batch_size` means pipeline batch size.

**Files:**
- Modify: `core/src/datayoga_core/blocks/azure/read_event_hub/block.py`
- Modify: `core/src/datayoga_core/blocks/azure/read_event_hub/block.schema.json`

- [ ] **Step 9.1: Write the failing test**

Create `core/src/datayoga_core/blocks/azure/read_event_hub/tests/__init__.py` (empty) and `core/src/datayoga_core/blocks/azure/read_event_hub/tests/test_event_hub.py`:

```python
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
    """additionalProperties: false catches typos like the legacy 'batch_sz'."""
    with pytest.raises(ValidationError):
        Block(_minimal_props({"batch_sz": 300}))


def test_max_batch_size_accepted():
    """The renamed SDK-level property is now max_batch_size."""
    block = Block(_minimal_props({"max_batch_size": 500, "batch_size": 100}))
    assert block.properties["max_batch_size"] == 500
    assert block.properties["batch_size"] == 100


def test_max_batch_size_defaults_to_300_when_omitted():
    """init() reads max_batch_size with a default of 300 if not present."""
    # We can't safely call init() in unit tests (it instantiates the Azure
    # SDK client); read the property via the same path init() does.
    block = Block(_minimal_props())
    assert int(block.properties.get("max_batch_size", 300)) == 300


def test_renamed_schema_has_additional_properties_false():
    """Schema after rename: max_batch_size + streamable's batch_size/flush_ms,
    no unknown properties allowed."""
    block = Block(_minimal_props())
    schema = block.get_json_schema()
    assert schema.get("additionalProperties") is False
    assert "max_batch_size" in schema["properties"]
    assert "batch_size" in schema["properties"]  # from streamable fragment
    assert "flush_ms" in schema["properties"]    # from streamable fragment


def test_batch_size_300_is_silently_repurposed():
    """A user upgrading from a pre-rename version with batch_size: 300 (which
    used to mean SDK callback size) will see their YAML still validate, but
    batch_size now means pipeline batch size. This is documented in the PR
    description and processing-strategies.md as a breaking change."""
    block = Block(_minimal_props({"batch_size": 300}))
    # Schema validation passes — batch_size is a known property (now pipeline-meaning).
    # The user must rename to max_batch_size: 300 to preserve old behavior.
    assert block.properties["batch_size"] == 300
    assert "max_batch_size" not in block.properties
```

- [ ] **Step 9.2: Run test to verify it fails**

```bash
cd core && python -m pytest src/datayoga_core/blocks/azure/read_event_hub/tests/test_event_hub.py -v
```

Expected: most of the 5 tests FAIL — current schema has no `additionalProperties: false`, no `max_batch_size`, no `$inherit`.

- [ ] **Step 9.3: Update the schema**

Replace `core/src/datayoga_core/blocks/azure/read_event_hub/block.schema.json` with:

```json
{
  "title": "azure.read_event_hub",
  "description": "Read from Azure Event Hub",
  "type": "object",
  "$inherit": ["streamable"],
  "properties": {
    "event_hub_connection_string": {
      "type": "string",
      "description": "The connection string for the Azure Event Hub namespace."
    },
    "event_hub_consumer_group_name": {
      "type": "string",
      "description": "The name of the consumer group to read events from."
    },
    "event_hub_name": {
      "type": "string",
      "description": "The name of the Azure Event Hub."
    },
    "checkpoint_store_connection_string": {
      "type": "string",
      "description": "The connection string for the Azure Storage account used as the checkpoint store."
    },
    "checkpoint_store_container_name": {
      "type": "string",
      "description": "The name of the container within the checkpoint store to store the checkpoints."
    },
    "max_batch_size": {
      "type": "integer",
      "minimum": 1,
      "description": "Maximum number of events to receive in each SDK callback. Renamed from the previous batch_size which used to mean this. Defaults to 300.",
      "default": 300
    }
  },
  "additionalProperties": false,
  "required": [
    "event_hub_connection_string",
    "event_hub_consumer_group_name",
    "event_hub_name",
    "checkpoint_store_connection_string",
    "checkpoint_store_container_name"
  ]
}
```

- [ ] **Step 9.4: Migrate the producer**

Replace the contents of `core/src/datayoga_core/blocks/azure/read_event_hub/block.py` with:

```python
import asyncio
import logging
from typing import Any, AsyncGenerator, Dict, List, Optional

import orjson
from azure.eventhub import EventData, PartitionContext
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import \
    BlobCheckpointStore
from datayoga_core.context import Context
from datayoga_core.producer import Producer as DyProducer

logger = logging.getLogger("dy")


class Block(DyProducer):
    """Azure Event Hub block for reading events."""

    DEFAULT_FLUSH_MS = 1000

    def init(self, context: Optional[Context] = None):
        logger.debug(f"Initializing {self.get_block_name()}")
        self.max_batch_size = int(self.properties.get("max_batch_size", 300))
        self.consumer_client = EventHubConsumerClient.from_connection_string(
            conn_str=self.properties["event_hub_connection_string"],
            consumer_group=self.properties["event_hub_consumer_group_name"],
            eventhub_name=self.properties["event_hub_name"],
            checkpoint_store=BlobCheckpointStore.from_connection_string(
                self.properties["checkpoint_store_connection_string"],
                self.properties["checkpoint_store_container_name"]),
        )
        self.events: Dict[Any, Any] = {}
        self.messages: asyncio.Queue = asyncio.Queue()

    async def produce_chunks(self) -> AsyncGenerator[List[Dict[str, Any]], None]:
        logger.debug(f"Running {self.get_block_name()}")
        logger.debug("Starting event receiving process")
        asyncio.create_task(self.receive_batch())

        while True:
            first = await self.messages.get()
            chunk = [first]
            while not self.messages.empty():
                chunk.append(self.messages.get_nowait())
            yield chunk

    async def receive_batch(self):
        await self.consumer_client.receive_batch(
            on_event_batch=self.on_event_batch,
            max_batch_size=self.max_batch_size,
            starting_position="-1",
        )

    async def on_event_batch(self, partition_context: PartitionContext, events: List[EventData]):
        logger.debug(f"Received batch of events from partition: {partition_context.partition_id}")
        for event in events:
            try:
                payload = orjson.loads(event.body_as_str(encoding="UTF-8"))
                msg_id = event.system_properties[b"x-opt-sequence-number"]
                self.events[msg_id] = (event, partition_context)
                payload[self.MSG_ID_FIELD] = msg_id
                await self.messages.put(payload)
            except Exception as e:
                logger.error(e)

    async def complete_events(self, msg_ids: List[str]):
        for msg_id in msg_ids:
            logger.debug(f"Acking {msg_id} event")
            event, partition_context = self.events.pop(msg_id, (None, None))
            if event is not None:
                await partition_context.update_checkpoint(event)
            else:
                logger.warning(f"Couldn't find event {msg_id} for acknowledging")

    def ack(self, msg_ids: List[str]):
        asyncio.create_task(self.complete_events(msg_ids))
```

- [ ] **Step 9.5: Run test to verify it passes**

```bash
cd core && python -m pytest src/datayoga_core/blocks/azure/read_event_hub/tests/test_event_hub.py -v
```

Expected: 5 passed.

- [ ] **Step 9.6: Run the full core suite**

```bash
cd core && python -m pytest src/datayoga_core/ -x -q
```

Expected: all tests pass.

- [ ] **Step 9.7: Commit**

```bash
git add core/src/datayoga_core/blocks/azure/read_event_hub/block.py \
        core/src/datayoga_core/blocks/azure/read_event_hub/block.schema.json \
        core/src/datayoga_core/blocks/azure/read_event_hub/tests/__init__.py \
        core/src/datayoga_core/blocks/azure/read_event_hub/tests/test_event_hub.py
git commit -m "Migrate azure/read_event_hub; rename batch_size -> max_batch_size (#400, BREAKING)"
```

---

## Task 10: Regenerate autogenerated schemas and docs

The aggregated `schemas/job.schema.json` and the per-block markdown in `docs/reference/blocks/` are generated by scripts. After the per-block schema changes, regenerate them.

**Files:**
- Modify: `schemas/job.schema.json`
- Modify: `docs/reference/blocks/std_read.md`, `files_read_csv.md`, `parquet_read.md`, `relational_read.md`, `redis_read_stream.md`, `http_receiver.md`, `azure_read_event_hub.md` (autogenerated)

- [ ] **Step 10.1: Regenerate the JSON schemas**

```bash
bash scripts/generate-jsonschemas.sh
```

Expected output: `JSON schemas generated successfully`.

- [ ] **Step 10.2: Regenerate the reference docs**

```bash
bash scripts/generate-docs.sh
```

Expected: completes without error.

- [ ] **Step 10.3: Inspect the diff**

```bash
git diff schemas/ docs/reference/blocks/ | head -200
```

Expected: `batch_size` (and `flush_ms` for streaming producers, `fetch_size` for relational/read, `max_batch_size` for event_hub) appear in the appropriate schema entries and docs.

- [ ] **Step 10.4: Commit**

```bash
git add schemas/job.schema.json docs/reference/blocks/
git commit -m "Regenerate JSON schemas and reference docs after producer batching (#400)"
```

---

## Task 11: Document the producer batching model in processing-strategies

**Files:**
- Modify: `docs/processing-strategies.md`

- [ ] **Step 11.1: Add a section on producer batching**

Append the following section to `docs/processing-strategies.md` (or replace an existing section if one already covers it):

````markdown
## Producer Batching

Every producer block (any block that reads from a source — `std/read`, `files/read_csv`, `parquet/read`, `relational/read`, `redis/read_stream`, `azure/read_event_hub`, `http/receiver`) accepts a `batch_size` property. The producer base class re-chunks the source's output into batches of exactly `batch_size` records, regardless of how the source delivers them (per row, per row group, per `fetchmany`, per network message).

```yaml
input:
  uses: files.read_csv
  with:
    file: people.csv
    batch_size: 500   # downstream steps process 500 records per call
```

Default: `1000`.

### Streaming producers and `flush_ms`

Streaming producers (`redis/read_stream`, `azure/read_event_hub`, `http/receiver`) also accept `flush_ms`. If no new records arrive within that many milliseconds, any partial batch is flushed downstream instead of being held until `batch_size` is reached.

```yaml
input:
  uses: redis.read_stream
  with:
    connection: my_redis
    stream_name: events
    batch_size: 1000
    flush_ms: 500   # emit a partial batch after 500ms of inactivity
```

Default: `1000` ms. Set to `null` to disable time-based flushing (records are held until `batch_size` or end-of-stream).

### `relational/read` and `fetch_size`

`relational/read` exposes an extra `fetch_size` property that controls how many rows are pulled from the database driver per round-trip, independent of the pipeline `batch_size`. Default: `10000`. Tune lower for memory pressure with wide rows; tune higher if you want fewer DB round-trips and downstream processing is the bottleneck.

### `azure/read_event_hub` migration note

In earlier versions, `batch_size` on `azure/read_event_hub` controlled the SDK callback batch size, not the pipeline batch size. As of #400 it has been renamed to `max_batch_size` to match the SDK semantic, and `batch_size` now consistently means pipeline batch size as it does for every other producer.
````

- [ ] **Step 11.2: Commit**

```bash
git add docs/processing-strategies.md
git commit -m "Document producer batching model in processing-strategies (#400)"
```

---

## Task 12: Full verification and push branch

- [ ] **Step 12.1: Run full core test suite**

```bash
cd core && python -m pytest src/datayoga_core/ -v
```

Expected: all tests pass. Notably:
- `test_producer_batching.py` (7 tests)
- `test_schema_inherit.py` (5 tests)
- `test_std_read.py`, `test_read_csv.py`, `test_parquet_read.py`, `test_relational_read.py`, `test_http_receiver.py`, `test_redis_read_stream.py`, `test_event_hub.py` (12 tests total)
- All pre-existing tests still pass.

- [ ] **Step 12.2: Inspect the branch's commit history**

```bash
git log --oneline 400-producer-batching-unification ^main
```

Expected: a clean sequence of commits — one per task — each referencing #400.

- [ ] **Step 12.3: Push the branch**

```bash
git push -u origin 400-producer-batching-unification
```

Expected: branch pushed to remote.

- [ ] **Step 12.4: Open a draft PR (deferred — confirm with user first)**

Before opening the PR, ask the user whether to open it as draft or ready-for-review, and confirm the body content. Do not run `gh pr create` autonomously.

The PR description should call out the breaking change explicitly (no CHANGELOG file exists in this repo, so the PR description is the canonical place):

> **Breaking change:** `azure/read_event_hub.batch_size` has been renamed to `max_batch_size`. The name `batch_size` now means pipeline batch size on this block, consistent with every other producer. Users with `batch_size: <N>` in their YAML for `azure/read_event_hub` must rename it to `max_batch_size: <N>` to preserve the previous SDK callback size semantic; the literal `batch_size: <N>` will validate but with the new pipeline-level meaning.
