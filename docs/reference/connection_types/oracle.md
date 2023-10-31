---
parent: Connection Types
grand_parent: Reference
---

# Oracle Database Configuration

Schema for configuring Oracle database connection parameters


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
|**host**|`string`|DB host<br/>|yes|
|**port**|`integer`|DB port<br/>Minimum: `1`<br/>Maximum: `65535`<br/>|no|
|**driver**|`string`|Driver<br/>|no|
|**database**|`string`|DB name<br/>|yes|
|**user**|`string`|DB user<br/>|yes|
|**password**|`string`|DB password<br/>|no|
|**oracle\_thick\_mode**|`boolean`|Enable oracle's thick mode(requires installed Oracle Client)<br/>Default: `false`<br/>|no|
|**oracle\_thick\_mode\_lib\_dir**|`string`|Path to Oracle Client libraries<br/>|no|
|[**connect\_args**](#connect_args)|`object`|Additional arguments to use when connecting to the DB<br/>|no|
|[**query\_args**](#query_args)|`object`|Additional query string arguments to use when connecting to the DB<br/>|no|

**Example**

```yaml
hr:
  type: oracle
  host: localhost
  port: 5432
  database: orcl
  user: scott
  password: tiger
  oracle_thick_mode: true
  oracle_thick_mode_lib_dir: /opt/oracle/instantclient_21_8/

```

<a name="connect_args"></a>
## connect\_args: object

Additional arguments to use when connecting to the DB


**Additional Properties:** allowed  
<a name="query_args"></a>
## query\_args: object

Additional query string arguments to use when connecting to the DB


**Additional Properties:** allowed  

