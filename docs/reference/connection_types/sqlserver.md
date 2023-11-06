---
parent: Connection Types
grand_parent: Reference
---

# SQL Server

Schema for configuring SQL Server database connection parameters


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**type**|`string`|Connection type<br/>Constant Value: `"sqlserver"`<br/>|yes|
|**host**|`string`|DB host<br/>|yes|
|**port**|`integer`|DB port<br/>Minimum: `1`<br/>Maximum: `65535`<br/>|no|
|**database**|`string`|DB name<br/>|yes|
|**user**|`string`|DB user<br/>|yes|
|**password**|`string`|DB password<br/>|no|
|[**connect\_args**](#connect_args)|`object`|Additional arguments to use when connecting to the DB<br/>|no|
|[**query\_args**](#query_args)|`object`|Additional query string arguments to use when connecting to the DB<br/>|no|

**Example**

```yaml
hr:
  type: sqlserver
  host: localhost
  port: 1433
  database: hr
  user: myuser
  password: mypass

```

<a name="connect_args"></a>
## connect\_args: object

Additional arguments to use when connecting to the DB


**Additional Properties:** allowed  
<a name="query_args"></a>
## query\_args: object

Additional query string arguments to use when connecting to the DB


**Additional Properties:** allowed  

