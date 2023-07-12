---
parent: Blocks
grand_parent: Reference
---

# azure\.read\_event\_hub

Read from Azure Event Hub


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**event\_hub\_connection\_string**|`string`|The connection string for the Azure Event Hub namespace.<br/>|yes|
|**event\_hub\_consumer\_group\_name**|`string`|The name of the consumer group to read events from.<br/>|yes|
|**event\_hub\_name**|`string`|The name of the Azure Event Hub.<br/>|yes|
|**checkpoint\_store\_connection\_string**|`string`|The connection string for the Azure Storage account used as the checkpoint store.<br/>|yes|
|**checkpoint\_store\_container\_name**|`string`|The name of the container within the checkpoint store to store the checkpoints.<br/>|yes|
|**batch\_size**|`integer`|The maximum number of events to receive in each batch.<br/>Default: `300`<br/>|no|

**Example**

```yaml
batch_size: 300

```


