# Producer batching unification

**Status:** Implemented in PR #401
**Date:** 2026-05-28
**Issue:** #400
**Closes:** #293, #294, #295, #296, #377 (as a side effect of the refactor)

## Problem

Seven producer blocks each handle (or fail to handle) batching differently:

| Producer               | Bounded/Streaming | `batch_size` today                                                                    | Behavior                                   |
| ---------------------- | ----------------- | ------------------------------------------------------------------------------------- | ------------------------------------------ |
| `std/read`             | bounded           | yes, default 1000 _(on `batch_size_in_std_read_block` branch)_                        | custom `process_batch` accumulator         |
| `files/read_csv`       | bounded           | yes, default 1000                                                                     | own `islice(reader, batch_size)` loop      |
| `relational/read`      | bounded           | **no** — hardcoded `fetchmany(10000)`                                                 | yields one row at a time downstream (bug)  |
| `parquet/read`         | bounded           | **no**                                                                                | yields one row at a time (bug)             |
| `redis/read_stream`    | streaming         | **no**                                                                                | yields one record at a time (bug #377)     |
| `azure/read_event_hub` | streaming         | yes, default 300, **but** controls _SDK callback batch size_, not pipeline batch size | drains internal queue in unbounded batches |
| `http/receiver`        | streaming         | **no**                                                                                | yields one record per HTTP request (bug)   |

Four are actively buggy (yielding single records into the pipeline when batches are intended). One uses `batch_size` with a different semantic. Each producer that has implemented batching has done it differently.

The duplication is the root cause of issues #294, #295, #296, and #377 — all four are the same gap, in different blocks.

## Goal

Make the `Producer` base class own batching. Subclasses describe how to fetch records; the base class controls the size and timing of batches yielded to the pipeline.

After the change:

- `batch_size` means the same thing in every producer: the maximum number of records yielded per downstream batch.
- Adding a new producer cannot reintroduce the "yield single records" bug — there's no place for it to happen.
- Streaming producers get an optional `flush_ms` so partial batches flush on inactivity instead of being held indefinitely.

Non-goals: changing the `Job`/`Step` pipeline, adding new sources, restructuring the Result/payload model (that's #245).

## Design

### Base-class contract

```python
# core/src/datayoga_core/producer.py

class Producer(Block):
    DEFAULT_BATCH_SIZE = 1000
    DEFAULT_FLUSH_MS = None  # streaming subclasses override

    @abstractmethod
    async def produce_chunks(self) -> AsyncGenerator[List[Message], None]:
        """Yield natural chunks of any size. Base class re-chunks to batch_size."""
        raise NotImplementedError

    async def produce(self) -> AsyncGenerator[List[Message], None]:
        """Public entry point. Reads chunks from produce_chunks() and re-emits
        in batches of up to batch_size (smaller on EOS or flush_ms), with
        bounded backpressure and source-error propagation."""
        ...
```

Subclasses override `produce_chunks` instead of `produce`. They emit chunks of any size — whatever's natural to the source (a Parquet row group, a `fetchmany` result, an `xreadgroup` response, an Event Hub callback batch, a single record).

The base class accumulates chunks and re-emits them in batches of up to `batch_size`, flushing whatever's left on end-of-stream and (for streaming sources) on `flush_ms` inactivity.

### `batch_size` and `flush_ms` are read lazily

`produce()` reads `self.properties.get("batch_size", self.DEFAULT_BATCH_SIZE)` on first call, not in `init()`. This avoids the "subclass forgot `super().init(context)`" footgun.

### `flush_ms` implementation

For streaming sources, partial batches must flush on inactivity, otherwise a low-traffic stream could hold records indefinitely.

Implementation uses an internal **bounded** queue + background pump task. The pump captures source errors and re-raises on the consumer side, so failures aren't silently treated as EOS:

```python
async def produce(self) -> AsyncGenerator[List[Dict[str, Any]], None]:
    batch_size = int(self.properties.get("batch_size", self.DEFAULT_BATCH_SIZE))
    flush_ms = self.properties.get("flush_ms", self.DEFAULT_FLUSH_MS)
    timeout = (flush_ms / 1000) if flush_ms else None

    # maxsize=1 preserves the natural backpressure the old yield-driven model
    # had: the pump can be at most one chunk ahead of the consumer.
    queue: asyncio.Queue = asyncio.Queue(maxsize=1)
    EOS = object()
    pump_error: List[BaseException] = []  # captured non-cancellation errors

    async def pump():
        cancelled = False
        try:
            async for chunk in self.produce_chunks():
                if chunk:
                    await queue.put(chunk)
        except asyncio.CancelledError:
            cancelled = True
            raise
        except BaseException as exc:
            pump_error.append(exc)
        finally:
            # Skip the EOS put on cancellation — the consumer's finally is
            # awaiting us and the queue may be full; putting would deadlock.
            if not cancelled:
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
                if pump_error:
                    raise pump_error[0]  # propagate source error to caller
                return

            buffer.extend(item)
            while len(buffer) >= batch_size:
                yield buffer[:batch_size]
                buffer = buffer[batch_size:]
    finally:
        pump_task.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await pump_task
```

Why a queue and not `asyncio.wait_for(anext(gen), timeout)`: cancelling `__anext__` on an async generator with side effects (open connections, partial reads) can leave it in a broken state. Cancelling the _pump task_ boundary is safe; the generator finishes its current chunk before the pump's `try/finally` runs.

Why `maxsize=1` and the `cancelled` flag: an unbounded queue removes backpressure — the pump could pre-load an entire parquet or relational table into memory while the consumer is processing batch 1 (flagged by Copilot review). Bounding at 1 keeps memory flat at the cost of a deadlock when the consumer is cancelled mid-flow (the pump's `finally: put(EOS)` blocks against a full queue). The `cancelled` flag skips the EOS put on cancellation, since the consumer is gone and EOS doesn't need to be delivered.

Why `pump_error`: catching all exceptions in the pump and letting it terminate via EOS would silently truncate input on a source failure (Redis disconnect, broken CSV, DB error) — the consumer would see clean end-of-stream against partial data. Capturing the exception and re-raising on the consumer side makes the job fail loudly instead (also flagged by Copilot review).

`flush_ms = None` ⇒ `timeout = None` ⇒ `queue.get()` waits forever ⇒ no time-based flush. Bounded sources don't set `flush_ms` and aren't affected.

### Schema composition (standard JSON Schema)

Two shared fragments in `core/src/datayoga_core/resources/schemas/` declare the common properties:

- `batchable.schema.json` declares `batch_size`.
- `streamable.schema.json` declares both `batch_size` and `flush_ms`.

Each block schema uses standard JSON Schema composition: `allOf` + `$ref` to the fragment file, plus `unevaluatedProperties: false` (rather than `additionalProperties: false`) so the fragment-contributed properties are recognized as evaluated. Example:

```json
{
  "$schema": "https://json-schema.org/draft/2019-09/schema",
  "title": "std.read",
  "type": "object",
  "allOf": [{ "$ref": "../../../resources/schemas/batchable.schema.json" }],
  "properties": {},
  "unevaluatedProperties": false
}
```

At load time, `schema_utils.resolve_refs(schema, schema_path)` walks the schema, finds any local-file `$ref` (relative path, ends in `.json`, no URI scheme, no in-document fragment), and inlines the referenced file's contents in place. The resulting in-memory schema is self-contained — no remaining `$ref`s — so `Block.validate()` keeps using the simple `jsonschema.validate(instance, schema)` code path. The on-disk schemas remain standards-compliant; the resolution is purely a runtime detail to avoid threading a `RefResolver` through every validation site.

`unevaluatedProperties: false` (introduced in draft 2019-09) is what makes composition + strict property validation work: with `additionalProperties: false`, a property contributed by an `allOf` member would be rejected as "additional" at the parent level. `unevaluatedProperties` is composition-aware.

External tools that ARE `$ref`-aware (IDE schema validators, OpenAPI exporters) read the on-disk schemas correctly without our resolver. The `jsonschema2mk` docs generator is not `$ref`-aware, so `scripts/generate-docs.sh` pre-resolves `$ref` and flattens `allOf` properties for docs rendering only.

### Per-producer changes

**`std/read`** (bounded)

Replace `process_batch` with a single-chunk yield. Base class slices.

```python
async def produce_chunks(self):
    if select.select([sys.stdin], [], [], 0.0)[0]:
        all_records = [r for line in sys.stdin for r in self.get_records(line)]
    else:
        print("Enter data to process:")
        all_records = self.get_records(input())
    if all_records:
        yield [self.get_message(r) for r in all_records]
```

**`files/read_csv`** (bounded)

Drops the `islice` loop; yield in `batch_size` chunks. Base class re-emits.

```python
async def produce_chunks(self):
    batch_size = int(self.properties.get("batch_size", self.DEFAULT_BATCH_SIZE))
    with open(self.file, "r", encoding=self.encoding) as f:
        reader = DictReader(f, fieldnames=self.fields,
                            delimiter=self.delimiter, quotechar=self.quotechar)
        for _ in range(self.skip):
            next(reader, None)
        counter = iter(count())
        while True:
            chunk = [{self.MSG_ID_FIELD: f"{next(counter)}", **r}
                     for r in islice(reader, batch_size)]
            if not chunk:
                return
            yield chunk
```

**`relational/read`** (bounded)

`batch_size` uses the framework default (1000). `fetch_size` defaults to **10000** to preserve today's driver-roundtrip count as the no-config baseline. Result: strict improvement vs. today (downstream goes from 1-record batches to 1000-record batches; DB roundtrips stay at 10000).

```python
class Block(DyProducer):
    DEFAULT_FETCH_SIZE = 10000

    async def produce_chunks(self):
        fetch_size = int(self.properties.get("fetch_size", self.DEFAULT_FETCH_SIZE))
        result = self.connection.execution_options(stream_results=True).execute(self.tbl.select())
        while True:
            rows = result.fetchmany(fetch_size)
            if not rows:
                return
            yield [utils.add_uid(dict(r._asdict())) for r in rows]
```

Schema adds optional `fetch_size` with default 10000.

**`parquet/read`** (bounded)

Fix one-by-one yield. Each row group becomes one chunk; base class re-emits in `batch_size` slices.

```python
async def produce_chunks(self):
    pf = ParquetFile(self.file)
    counter = iter(count())
    for df in pf.iter_row_groups():
        yield [{self.MSG_ID_FIELD: str(next(counter)), **row.to_dict()}
               for _, row in df.iterrows()]
```

**`redis/read_stream`** (streaming, closes #377)

Use `count=batch_size` on `xreadgroup`. Yield each batch as a chunk. Class overrides `DEFAULT_FLUSH_MS = 1000`.

```python
class Block(DyProducer):
    DEFAULT_FLUSH_MS = 1000

async def produce_chunks(self):
    batch_size = int(self.properties.get("batch_size", self.DEFAULT_BATCH_SIZE))
    read_pending = True
    while True:
        streams = self.redis_client.xreadgroup(
            self.consumer_group, self.requesting_consumer,
            {self.stream: "0" if read_pending else ">"},
            count=batch_size,
            block=100 if self.snapshot else 0,  # streaming blocks forever; snapshot polls briefly
        )
        for stream in streams:
            chunk = []
            for key, value in stream[1]:
                payload = orjson.loads(value[next(iter(value))])
                payload[self.MSG_ID_FIELD] = key
                chunk.append(payload)
            if chunk:
                yield chunk
        if self.snapshot and not read_pending:
            return
        read_pending = False
```

`flush_ms` (default 1000) ensures partial batches flush during low-volume periods. The pump task can sit blocked inside `xreadgroup` indefinitely — that's fine, because the pump and the consumer side of the base-class queue are decoupled. When a single message finally arrives, it lands in the queue immediately and `flush_ms` flushes the partial batch downstream.

**`azure/read_event_hub`** (streaming, breaking change)

Existing `batch_size` property → renamed `max_batch_size` (matches SDK semantic, default 300). New `batch_size` (pipeline semantic, default 1000) comes from the streamable fragment.

```python
class Block(DyProducer):
    DEFAULT_FLUSH_MS = 1000

    def init(self, context=None):
        self.max_batch_size = int(self.properties.get("max_batch_size", 300))
        # ... existing client setup ...
        self.events = {}
        self.messages = asyncio.Queue()

    async def produce_chunks(self):
        asyncio.create_task(self.receive_batch())  # uses self.max_batch_size
        while True:
            msg = await self.messages.get()
            chunk = [msg]
            while not self.messages.empty():
                chunk.append(self.messages.get_nowait())
            yield chunk
```

**Migration:** Users with `batch_size: 300` in YAML thinking it controls SDK callbacks must rename to `max_batch_size: 300`. No backward-compat shim. The literal `batch_size: 300` still validates after the rename but now means pipeline batch size, not SDK callback size — that semantic shift is documented in the PR description.

The schema for `azure/read_event_hub` also gains `unevaluatedProperties: false` (it had no `additionalProperties` declaration before). Typos like `batch_sz: 300` now fail validation loudly with a clear error.

**`http/receiver`** (streaming)

Drain the queue per chunk; `flush_ms` flushes partial batches when traffic is low.

```python
class Block(DyProducer):
    DEFAULT_FLUSH_MS = 1000

    async def produce_chunks(self):
        queue: Queue = Queue(maxsize=1000)
        async def handler(request):
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
        try:
            counter = iter(count())
            while True:
                msg = await queue.get()
                chunk = [{self.MSG_ID_FIELD: f"{next(counter)}", **msg}]
                while not queue.empty():
                    chunk.append({self.MSG_ID_FIELD: f"{next(counter)}", **queue.get_nowait()})
                yield chunk
        finally:
            with suppress(Exception):
                await srv.stop()
```

### Defaults summary

| Producer               | `batch_size` | `flush_ms` | Other                                                |
| ---------------------- | ------------ | ---------- | ---------------------------------------------------- |
| `std/read`             | 1000         | —          | —                                                    |
| `files/read_csv`       | 1000         | —          | —                                                    |
| `relational/read`      | 1000         | —          | optional `fetch_size`, defaults to 10000             |
| `parquet/read`         | 1000         | —          | —                                                    |
| `redis/read_stream`    | 1000         | 1000       | —                                                    |
| `azure/read_event_hub` | 1000         | 1000       | `max_batch_size` 300 (renamed from old `batch_size`) |
| `http/receiver`        | 1000         | 1000       | —                                                    |

## Tests

**New base-class tests** (`core/src/datayoga_core/tests/test_producer_batching.py`):

A `FakeProducer` whose `produce_chunks` yields scripted chunks. Cases:

- One 5000-record chunk + `batch_size=1000` → five batches of 1000.
- Three chunks of [200, 300, 400] + `batch_size=1000` → one batch of 900 on EOS (no empty trailing).
- 1500 records + `batch_size=1000` → batches of [1000, 500].
- `flush_ms=100` with a producer that sleeps 200ms between chunks → partial batches flush on inactivity.
- `flush_ms=None` holds records indefinitely (asserted with a timeout that the next batch doesn't arrive early).
- Empty chunk yields are ignored (no empty batches emitted).
- Pump-task cleanup: cancelling the consumer cancels the pump cleanly (no warnings, no leaks).

**Per-producer tests:**

- `std/read`, `files/read_csv` — existing tests adapted; assert batch counts/sizes match `batch_size`.
- `relational/read` — assert it yields batches (not single rows); assert `fetch_size` controls driver calls independently of `batch_size`.
- `parquet/read` — multi-row-group file; batches honor `batch_size` regardless of row-group boundaries.
- `redis/read_stream` — assert `xreadgroup` called with `count=batch_size`. The `redis_to_relational` integration test (mentioned in #377) provides the end-to-end signal; it depends on the batch-fallback in `relational/write` shipped in commit `7e5b6f7`, which is already in place.
- `azure/read_event_hub` — assert validation rejects legacy `batch_size: 500` with no `max_batch_size`; assert `max_batch_size: 500, batch_size: 100` results in SDK callbacks of 500 and downstream batches of 100.
- `http/receiver` — send N records via webhook; assert they land in batches of `batch_size`, or partial batches after `flush_ms`.

## Documentation

- Update `docs/reference/blocks/*_read.md` for each affected producer (`batch_size`, `flush_ms`, `fetch_size`, `max_batch_size` where applicable).
- Add a section in `docs/processing-strategies.md` explaining the producer batching model: chunked subclass output, base-class re-chunking, `flush_ms` for streaming sources.
- PR description carries the breaking-change note (no CHANGELOG file in this repo):
  - New `batch_size`/`flush_ms` on previously non-batching producers.
  - **Breaking:** `azure/read_event_hub.batch_size` renamed to `max_batch_size`; the name `batch_size` now means pipeline batch size.

## Risks and trade-offs

1. **`Producer` ABC change.** `produce_chunks` is the new override hook (raises NotImplementedError by default; not formally `@abstractmethod` so legacy subclasses that still override `produce()` directly continue to validate). All 7 in-tree producers were migrated to override `produce_chunks`; external/downstream subclassers that override `produce()` directly continue to work but bypass the base-class batching. Called out in the PR description.

2. **Event Hub silent-semantic-change risk.** The breaking rename is intentional. Adding `unevaluatedProperties: false` to the Event Hub schema (which lacked any `additionalProperties` declaration before) catches typos loudly. The literal `batch_size: 300` still validates after the rename but now means pipeline batch size, not SDK callback size — that semantic shift is documented in the PR description and the processing-strategies docs.

3. **`flush_ms` semantics on Job shutdown.** When the producer is being cancelled (`Job.shutdown` → `Step.stop`), the pump's `try/finally` ensures `EOS` is queued. The `produce()` loop sees `EOS` and flushes the final partial batch. Verified by the `test_producer_batching` shutdown case.

4. **`relational/read` defaults.** `fetch_size` defaults to 10000 to preserve today's DB roundtrip count. `batch_size` defaults to 1000, matching the framework default. Net effect vs. today: downstream batches grow from 1 to 1000 (huge improvement); DB roundtrips unchanged. Users with memory pressure on large rows can set a smaller `fetch_size` explicitly. Documented in the block's reference page.

5. **Re-chunking cost.** Lists are sliced with `buffer[:n]` / `buffer[n:]` — O(batch_size) per batch. Negligible relative to per-record block work; no benchmark required.

## Out of scope

- Changing the `Result`/payload internal field representation (issue #245).
- Adding new connector blocks (Snowflake #392, Kafka, S3 #351, RabbitMQ #265, Kinesis #264).
- Pulling Prometheus out of core (#336).
- Backpressure / queue sizing changes to the `Step` pipeline.
