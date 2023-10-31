---
parent: Connection Types
grand_parent: Reference
---

# Cassandra

Schema for configuring Cassandra database connection parameters


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**type**|`string`|DB type<br/>Enum: `"cassandra"`<br/>|yes|
|[**hosts**](#hosts)|`string[]`|Cassandra hosts<br/>|yes|
|**port**|`integer`|Cassandra DB port<br/>Default: `9042`<br/>Minimum: `1`<br/>Maximum: `65535`<br/>|no|
|**database**|`string`|DB name<br/>|no|
|**user**|`string`|DB user<br/>|no|
|**password**|`string`|DB password<br/>|no|

**Additional Properties:** not allowed  
**Example**

```yaml
cache:
  type: cassandra
  hosts:
    - localhost
  port: 9042
  database: myDB
  user: myUser
  password: myPassword

```

<a name="hosts"></a>
## hosts\[\]: array

Cassandra hosts


**Items: Address of Cassandra node**

**Item Type:** `string`  

