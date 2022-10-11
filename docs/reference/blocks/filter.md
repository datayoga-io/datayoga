---
parent: Blocks
grand_parent: Reference
---

# Filter Records

Filter records


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**expression**|`string`|Expression<br/>|yes|
|**language**|`string`|Language<br/>Enum: `"jmespath"`, `"sql"`<br/>|yes|

**Additional Properties:** not allowed  
**Example**

```yaml
language: sql
expression: age>20

```


