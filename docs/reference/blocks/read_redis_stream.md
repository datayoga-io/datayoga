---
parent: Blocks
grand_parent: Reference
---

# Read Redis Stream

Read from Redis stream


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**connection**|`string`|Connection name<br/>|yes|
|**stream\_name**<br/>(Source stream name)|`string`|Source stream name<br/>|yes|
|**snapshot**<br/>(Snapshot current entries and quit)|`boolean`|Snapshot current entries and quit<br/>Default: `false`<br/>|no|

**Additional Properties:** not allowed  
**Example**

```yaml
snapshot: false

```


