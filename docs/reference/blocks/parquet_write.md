---
parent: Blocks
grand_parent: Reference
---

# parquet\.write

Write data to parquet


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**file**|`string`|Filename. Can contain a regexp or glob expression<br/>|yes|
|**batch\_size**|`number`|Number of records to read per batch<br/>Default: `"1000"`<br/>|no|

**Additional Properties:** not allowed  
**Example**

```yaml
file: data.parquet

```


