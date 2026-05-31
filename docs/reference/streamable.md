---
parent: Reference
nav_order: 4
---

# streamable

Streaming producer mixin: declares batch_size and flush_ms for producers reading from continuous sources.


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**batch\_size**|`integer`|Maximum number of records yielded per downstream batch.<br/>Default: `1000`<br/>Minimum: `1`<br/>||
|**flush\_ms**|`integer`, `null`|If set, flush a partial batch after this many ms of inactivity. null or omitted = wait until batch_size or end-of-stream.<br/>Default: `1000`<br/>Minimum: `1`<br/>||

**Example**

```yaml
batch_size: 1000
flush_ms: 1000

```


