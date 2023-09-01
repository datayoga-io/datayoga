---
parent: Blocks
grand_parent: Reference
---

# redis\.write

Write to a Redis data structure


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**connection**<br/>(Connection name)|`string`||yes|
|[**command**](#command)<br/>(Redis command)|`object`|Redis command expressions<br/>|yes|
|**field**<br/>(Target field)|`string`|The field to write the result to<br/>|yes|
|**reject\_on\_error**|`boolean`|Should reject a record on error or not in case if the key is of a different type<br/>Default: `false`<br/>|no|

**Additional Properties:** not allowed  
**Example**

```yaml
command: {}
reject_on_error: false

```

<a name="command"></a>
## command: Redis command

Redis command expressions


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|[**elements**](#commandelements)|`string[]`|The list of command's elements<br/>|yes|
|**language**|`string`|Language<br/>Enum: `"jmespath"`, `"sql"`<br/>|yes|

<a name="commandelements"></a>
### command\.elements\[\]: array

The list of command's elements


**Items**

**Item Type:** `string`  

