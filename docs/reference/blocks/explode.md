---
parent: Blocks
grand_parent: Reference
---

# Explode

Split a field into multiple records


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**field**|`string`|Field<br/>|yes|
|**target\_field**|`string`|Target field to add to the result. If omitted, the source field name will be used<br/>|no|
|**delimiter**|`string`|Delimiter to use for splitting the source field<br/>Default: `","`<br/>|no|

**Additional Properties:** not allowed  
**Example**

```yaml
field: features
target_field: feature
delimiter: ;

```


