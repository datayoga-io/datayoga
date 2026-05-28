---
parent: Blocks
grand_parent: Reference
---

# parquet\.read

Read data from parquet


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**batch\_size**|`integer`|Maximum number of records yielded per downstream batch.<br/>Default: `1000`<br/>Minimum: `1`<br/>|no|
|**file**|`string`|Filename. Can contain a regexp or glob expression<br/>|yes|

**Example**

```yaml
file: data.parquet

```


