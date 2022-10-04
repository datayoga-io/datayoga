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
|**read\_once**<br/>(Read the steam only once)|`boolean`|Read stream elements only once and quit or read infinitely<br/>Default: `false`<br/>|no|

**Additional Properties:** not allowed  
**Example**

```yaml
read_once: false

```


