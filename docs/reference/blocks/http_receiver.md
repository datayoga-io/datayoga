---
parent: Blocks
grand_parent: Reference
---

# http\.receiver

Receives HTTP requests and process the data.


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**batch\_size**|`integer`|Maximum number of records yielded per downstream batch.<br/>Default: `1000`<br/>Minimum: `1`<br/>||
|**flush\_ms**|`integer`, `null`|If set, flush a partial batch after this many ms of inactivity. null or omitted = wait until batch_size or end-of-stream.<br/>Default: `1000`<br/>Minimum: `1`<br/>||
|**host**|`string`|Host to listen<br/>Default: `"0.0.0.0"`<br/>||
|**port**|`integer`|Port to listen<br/>Default: `8080`<br/>||

**Example**

```yaml
host: localhost
port: 8080

```


