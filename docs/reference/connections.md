---
parent: Reference
---

# Connections

Connection catalog


**Items**

   
**Option 1 (optional):** 
SQL database


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**name**|`string`|Connection name<br/>|no|
|**type**|`string`|DB type<br/>Pattern: ^\(?\!redis$\)<br/>|yes|
|**host**|`string`|DB host<br/>|yes|
|**port**|`integer`|DB port<br/>Minimum: `1`<br/>Maximum: `65535`<br/>|no|
|**database**|`string`|DB name<br/>|yes|
|**user**|`string`|DB user<br/>|yes|
|**password**|`string`|DB password<br/>|no|

**Additional Properties:** not allowed  

   
**Option 2 (optional):** 
Redis


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**name**|`string`|Connection name<br/>|yes|
|**type**|`string`|DB type<br/>Enum: `"redis"`<br/>|yes|
|**host**|`string`|Redis DB host<br/>|yes|
|**port**|`integer`|Redis DB port<br/>Minimum: `1`<br/>Maximum: `65535`<br/>|yes|
|**password**|`string`|Redis DB password<br/>|no|

**Additional Properties:** not allowed  

**Example**

```yaml
- {}

```


