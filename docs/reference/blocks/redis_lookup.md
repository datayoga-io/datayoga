---
parent: Blocks
grand_parent: Reference
---

# redis\.lookup

Lookup data from redis by given command and key


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**connection**<br/>(Connection name)|`string`||yes|
|**cmd**<br/>(Redis command)|`string`|The command to execute<br/>|yes|
|[**args**](#args)<br/>(Redis command arguments)|`string[]`|The list of expressions produces arguments<br/>|yes|
|**language**|`string`|Language<br/>Enum: `"jmespath"`, `"sql"`<br/>|yes|
|**field**<br/>(Target field)|`string`|The field to write the result to<br/>|yes|

**Additional Properties:** not allowed  
<a name="args"></a>
## args\[\]: Redis command arguments

The list of expressions produces arguments


**Items**

**Item Type:** `string`  

