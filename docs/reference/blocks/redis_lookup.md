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
|**command**<br/>(Redis command)|`string`|Redis command<br/>Default: `"GET"`<br/>Enum: `"GET"`, `"HGETALL"`, `"SMEMBERS"`, `"ZRANGEBYSCORE"`, `"LRANGE"`, `"JSON.GET"`<br/>|no|
|[**key**](#key)|`object`|Field to use as the Redis key<br/>|yes|
|**field**<br/>(Target field)|`string`|The field to write the result to<br/>|yes|
|**reject\_on\_error**|`boolean`|Should reject a record on error or not in case if the key is of a different type<br/>Default: `true`<br/>|no|

**Additional Properties:** not allowed  
**Example**

```yaml
command: GET
key: {}
reject_on_error: true

```

<a name="key"></a>
## key: object

Field to use as the Redis key


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**expression**|`string`|Expression<br/>|yes|
|**language**|`string`|Language<br/>Enum: `"jmespath"`, `"sql"`<br/>|yes|


