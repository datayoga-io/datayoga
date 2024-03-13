---
parent: Blocks
grand_parent: Reference
---

# relational\.read

Read a table from an SQL-compatible data store


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**connection**<br/>(The connection to use for loading)|`string`|Logical connection name as defined in the connections.dy.yaml<br/>|yes|
|**schema**<br/>(The table schema of the table)|`string`|If left blank, the default schema of this connection will be used as defined in the connections.dy.yaml<br/>|no|
|**table**<br/>(The table name)|`string`|Table name<br/>|yes|
|[**columns**](#columns)<br/>(Optional subset of columns to load)|`array`||no|

**Additional Properties:** not allowed  
**Example**

```yaml
id: read_snowflake
type: relational.read
properties:
  connection: eu_datalake
  table: employees
  schema: dbo

```

<a name="columns"></a>
## columns\[\]: Optional subset of columns to load

**Items: name of column**

**No properties.**

**Example**

```yaml
- fname
- lname: last_name

```


