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
|**cmd**<br/>(Redis command)|`string`|The expression produces the command to execute<br/>|yes|
|[**args**](#args)<br/>(Redis command arguments)|`string[]`|The list of expressions produces arguments<br/>|yes|
|**language**|`string`|Language<br/>Enum: `"jmespath"`, `"sql"`<br/>|yes|
|**field**<br/>(Target field)|`string`|The field to write the result to<br/>|yes|
|**reject\_on\_error**|`boolean`|Should reject a record on error or not in case if the key is of a different type<br/>Default: `false`<br/>|no|

**Additional Properties:** not allowed  
**Example**

```yaml
reject_on_error: false

```

<a name="args"></a>
## args\[\]: Redis command arguments

The list of expressions produces arguments


**Items**

**Item Type:** `string`  

