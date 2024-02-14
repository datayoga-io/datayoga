---
parent: Connection Types
grand_parent: Reference
---

# Kafka

Schema for configuring Kafka connection parameters


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**type**|`string`|Connection type<br/>Constant Value: `"kafka"`<br/>|yes|
|**bootstrap\_servers**|`string`|Kafka Hosts<br/>|yes|

**Additional Properties:** not allowed  
**Example**

```yaml
kafka:
  type: kafka
  bootstrap_servers:
    - localhost:9092

```


