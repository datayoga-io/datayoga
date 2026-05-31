---
parent: Blocks
grand_parent: Reference
---

# redis\.read\_stream

Read from Redis stream


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**batch\_size**|`integer`|Maximum number of records yielded per downstream batch.<br/>Default: `1000`<br/>Minimum: `1`<br/>|no|
|**flush\_ms**|`integer`, `null`|If set, flush a partial batch after this many ms of inactivity. null or omitted = wait until batch_size or end-of-stream.<br/>Default: `1000`<br/>Minimum: `1`<br/>|no|
|**connection**|`string`|Connection name<br/>|yes|
|**stream\_name**<br/>(Source stream name)|`string`|Source stream name<br/>|yes|
|**snapshot**<br/>(Snapshot current entries and quit)|`boolean`|Snapshot current entries and quit<br/>Default: `false`<br/>|no|

**Example**

```yaml
batch_size: 1000
flush_ms: 1000
snapshot: false

```


