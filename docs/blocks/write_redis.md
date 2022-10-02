---
parent: Blocks Reference
layout: page
---

# Write Redis

Write to a Redis data structure


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**connection**|`string`|Connection name<br/>|yes|
|**command**<br/>(Redis command)|`string`|Redis command<br/>Enum: `"HSET"`, `"SADD"`, `"XADD"`, `"RPUSH"`, `"LPUSH"`, `"SET"`, `"ZADD"`<br/>|yes|
|**key\_field**|`string`|Field to use as the Redis key<br/>|yes|

**Additional Properties:** not allowed  

