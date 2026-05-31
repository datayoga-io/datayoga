---
parent: Reference
nav_order: 1
---

# batchable

Producer batching mixin: declares batch_size for producers that yield records in batches.


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**batch\_size**|`integer`|Maximum number of records yielded per downstream batch.<br/>Default: `1000`<br/>Minimum: `1`<br/>||

**Example**

```yaml
batch_size: 1000

```


