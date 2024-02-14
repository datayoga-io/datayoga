---
parent: Blocks
grand_parent: Reference
---

# kafka\.read

Read from the kafka consumer


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**bootstrap\_servers**|`string`|host name<br/>|yes|
|**topic**|`string`|Kafka topic<br/>|yes|
|**group**|`string`|Kafka group<br/>|no|
|**seek\_to\_beginning**|`boolean`|Consumer seek to beginning<br/>|no|
|**snapshot**<br/>(Snapshot current entries and quit)|`boolean`|Snapshot current entries and quit<br/>Default: `false`<br/>|no|

**Example**

```yaml
snapshot: false

```


