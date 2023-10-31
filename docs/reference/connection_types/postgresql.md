---
parent: Connection Types
grand_parent: Reference
---

# PostgreSQL Database Configuration

Schema for configuring PostgreSQL database connection parameters


**Properties**

|Name|Type|Description|Required|
|----|----|-----------|--------|
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
  type: postgresql
  host: localhost
  port: 5432
  database: postgres
  user: postgres
  password: postgres
  connect_args:
    connect_timeout: 10
  query_args:
    sslmode: verify-ca
    sslrootcert: /opt/ssl/ca.crt
    sslcert: /opt/ssl/client.crt
    sslkey: /opt/ssl/client.key

```

<a name="connect_args"></a>
## connect\_args: object

Additional arguments to use when connecting to the DB


**Additional Properties:** allowed  
<a name="query_args"></a>
## query\_args: object

Additional query string arguments to use when connecting to the DB


**Additional Properties:** allowed  

