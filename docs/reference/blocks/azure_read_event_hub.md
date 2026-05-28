---
parent: Blocks
grand_parent: Reference
---

# azure\.read\_event\_hub

Read from Azure Event Hub


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**batch\_size**|`integer`|Maximum number of records yielded per downstream batch.<br/>Default: `1000`<br/>Minimum: `1`<br/>|no|
|**flush\_ms**|`integer`, `null`|If set, flush a partial batch after this many ms of inactivity. null or omitted = wait until batch_size or end-of-stream.<br/>Default: `1000`<br/>Minimum: `1`<br/>|no|
|**event\_hub\_connection\_string**|`string`|The connection string for the Azure Event Hub namespace.<br/>|yes|
|**event\_hub\_consumer\_group\_name**|`string`|The name of the consumer group to read events from.<br/>|yes|
|**event\_hub\_name**|`string`|The name of the Azure Event Hub.<br/>|yes|
|**checkpoint\_store\_connection\_string**|`string`|The connection string for the Azure Storage account used as the checkpoint store.<br/>|yes|
|**checkpoint\_store\_container\_name**|`string`|The name of the container within the checkpoint store to store the checkpoints.<br/>|yes|
|**max\_batch\_size**|`integer`|Maximum number of events to receive in each SDK callback. Renamed from the previous batch_size which used to mean this. Defaults to 300.<br/>Default: `300`<br/>Minimum: `1`<br/>|no|

**Additional Properties:** not allowed  
**Example**

```yaml
batch_size: 1000
flush_ms: 1000
max_batch_size: 300

```


